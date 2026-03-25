use std::collections::HashMap;

use crate::{
    bus::BusClientSender,
    consensus::CommittedConsensusProposal,
    mempool::{mempool_bus_client::MempoolBusClient, storage::EntryOrMissingHash},
    model::*,
};
use futures::StreamExt;
use hyli_modules::{log_error, log_warn};

use super::{
    storage::{LaneEntryMetadata, MetadataOrMissingHash},
    DisseminationEvent, ValidatorDAG,
};
use anyhow::{bail, Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use hyli_model::{ArcBorsh, ArcSignedBlock};
use std::sync::Arc;
use tracing::{debug, error, info, trace, warn};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct BlockUnderConstruction {
    pub from: Option<Cut>,
    pub ccp: CommittedConsensusProposal,
    pub holes_tops: HashMap<LaneId, (DataProposalHash, LaneBytesSize)>,
    pub holes_materialized: bool,
}

struct HoleFillError {
    lane_id: LaneId,
    hole_top: DataProposalHash,
    lane_size: LaneBytesSize,
    err: anyhow::Error,
}

type HoleFillChainResult =
    std::result::Result<(LaneId, bool, Option<(DataProposalHash, LaneBytesSize)>), HoleFillError>;

impl super::Mempool {
    const CLEAN_AND_UPDATE_SCAN_LIMIT: usize = 100;

    pub(super) async fn on_sync_reply(
        &mut self,
        lane_id: &LaneId,
        sender_validator: &ValidatorPublicKey,
        signatures: Vec<ValidatorDAG>,
        data_proposal: impl Into<Arc<DataProposal>>,
    ) -> Result<()> {
        let data_proposal = data_proposal.into();
        debug!("SyncReply from validator {sender_validator}");

        self.metrics
            .sync_reply_receive(lane_id, self.crypto.validator_pubkey());
        self.metrics.add_consumed_data_proposal_bytes(
            lane_id,
            data_proposal.estimate_size() as u64,
            "sync_reply",
        );

        #[cfg(test)]
        {
            let dp_hash = data_proposal.hashed();
            self.on_hashed_sync_reply(lane_id.clone(), signatures, data_proposal, dp_hash)
                .await?;
        }
        #[cfg(not(test))]
        {
            let lane_id_clone = lane_id.clone();
            let handle = self.inner.long_tasks_runtime.handle();
            self.inner.processing_dps.spawn_on(
                async move {
                    let dp_hash = data_proposal.hashed();
                    Ok(crate::mempool::ProcessedDPEvent::OnHashedSyncReply((
                        lane_id_clone,
                        signatures,
                        data_proposal,
                        dp_hash,
                    )))
                },
                &handle,
            );
        }

        Ok(())
    }

    pub(super) async fn on_hashed_sync_reply(
        &mut self,
        lane_id: LaneId,
        signatures: Vec<ValidatorDAG>,
        data_proposal: Arc<DataProposal>,
        dp_hash: DataProposalHash,
    ) -> Result<()> {
        // We don't check if we already have stored the DP just in case
        // we processed it async between hole materialization and now.

        trace!(
            "Filling hole with 1 entry (parent dp hash: {:?}) for {lane_id}",
            data_proposal.parent_data_proposal_hash
        );

        if let Some(buc) = self.inner.blocks_under_contruction.iter_mut().find(|buc| {
            buc.holes_tops
                .get(&lane_id)
                .is_some_and(|(hole_top, ..)| hole_top == &dp_hash)
        }) {
            let (expected_top, lane_size) = buc
                .holes_tops
                .get(&lane_id)
                .context("Expected hole top missing")?
                .clone();
            let next_hole = Self::fill_hole_from_entry(
                &mut self.lanes,
                &mut self.bus,
                buc,
                &lane_id,
                &expected_top,
                lane_size,
                signatures,
                data_proposal,
            )?;
            match next_hole {
                Some((next_top, next_size)) => {
                    buc.holes_tops
                        .insert(lane_id.clone(), (next_top, next_size));
                }
                None => {
                    buc.holes_tops.remove(&lane_id);
                }
            }

            if buc.holes_tops.is_empty() {
                self.try_to_send_full_signed_blocks()
                    .await
                    .context("Try process queued CCP")?;
            }

            return Ok(());
        }

        // Didn't find the matching BUC
        debug!(
            "Buffering SyncReply entry for lane {} with hash {}",
            lane_id, dp_hash
        );
        self.buffered_entries
            .entry(lane_id.clone())
            .or_default()
            .insert(dp_hash.clone(), (signatures, data_proposal));

        Ok(())
    }

    // Consumes buffered entries if present before falling back to stored data.
    fn take_entry_for_hash(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Option<(Vec<ValidatorDAG>, Arc<DataProposal>)> {
        if let Some((signatures, data_proposal)) = self
            .buffered_entries
            .get_mut(lane_id)
            .and_then(|m| m.remove(dp_hash))
        {
            return Some((signatures, data_proposal));
        }

        if let (Ok(Some(metadata)), Ok(Some(data_proposal))) = (
            self.lanes.get_metadata_by_hash(lane_id, dp_hash),
            self.lanes.get_dp_by_hash(lane_id, dp_hash),
        ) {
            return Some((metadata.signatures, data_proposal.into()));
        }

        None
    }

    // Attempt to resolve all pending holes from buffered or locally stored data.
    fn resolve_all_holes(&mut self, buc: &mut BlockUnderConstruction) -> Result<bool> {
        if buc.holes_tops.is_empty() {
            return Ok(false);
        }

        // Work around copying keys by taking and recreating as needed.
        let fill_from = std::mem::take(&mut buc.holes_tops);
        let mut filled_any = false;
        let mut iter = fill_from.into_iter();
        while let Some((lane_id, (hole_top, lane_size))) = iter.next() {
            match self.fill_hole_chain(buc, lane_id, hole_top, lane_size) {
                Ok((lane_id, filled, next_hole)) => {
                    if filled {
                        filled_any = true;
                    }
                    if let Some((next_top, next_size)) = next_hole {
                        buc.holes_tops.insert(lane_id, (next_top, next_size));
                    }
                }
                Err(HoleFillError {
                    lane_id,
                    hole_top,
                    lane_size,
                    err,
                }) => {
                    // Reinsert remaining holes on error to avoid dropping state.
                    buc.holes_tops.insert(lane_id, (hole_top, lane_size));
                    for (lane_id, (hole_top, lane_size)) in iter {
                        buc.holes_tops.insert(lane_id, (hole_top, lane_size));
                    }
                    return Err(err);
                }
            }
        }

        Ok(filled_any)
    }

    // Core hole-fill loop shared by storage recovery/build path.
    fn fill_hole_chain(
        &mut self,
        buc: &mut BlockUnderConstruction,
        lane_id: LaneId,
        mut hole_top: DataProposalHash,
        mut lane_size: LaneBytesSize,
    ) -> HoleFillChainResult {
        let mut filled_any = false;
        loop {
            let Some((signatures, data_proposal)) = self.take_entry_for_hash(&lane_id, &hole_top)
            else {
                return Ok((lane_id, filled_any, Some((hole_top, lane_size))));
            };
            match Self::fill_hole_from_entry(
                &mut self.lanes,
                &mut self.bus,
                buc,
                &lane_id,
                &hole_top,
                lane_size,
                signatures,
                data_proposal,
            ) {
                Ok(next_hole) => match next_hole {
                    Some((next_top, next_size)) => {
                        filled_any = true;
                        hole_top = next_top;
                        lane_size = next_size;
                    }
                    None => {
                        return Ok((lane_id, true, None));
                    }
                },
                Err(err) => {
                    return Err(HoleFillError {
                        lane_id,
                        hole_top,
                        lane_size,
                        err,
                    });
                }
            }
        }
    }

    // Fill a single hole entry; returns the next hole top, if any.
    // One day rust will have disjoint self borrows.
    #[expect(clippy::too_many_arguments, reason = "Split to avoid double borrowing")]
    fn fill_hole_from_entry(
        lanes: &mut super::LanesStorage,
        bus: &mut MempoolBusClient,
        buc: &mut BlockUnderConstruction,
        lane_id: &LaneId,
        expected_top: &DataProposalHash,
        lane_size: LaneBytesSize,
        signatures: Vec<ValidatorDAG>,
        data_proposal: Arc<DataProposal>,
    ) -> Result<Option<(DataProposalHash, LaneBytesSize)>> {
        let lane_operator = lane_id.operator();
        let dp_hash = data_proposal.hashed();
        if expected_top != &dp_hash {
            bail!(
                "Hole fill expected top {} but got {} for lane {}",
                expected_top,
                dp_hash,
                lane_id
            );
        }
        // We have found a hole matching this DP - we don't really need to validate signatures to handle it,
        // as it was part of the cut, but as a warn if signatures don't match
        let found_operator_signature = signatures.iter().any(|s| {
            &s.signature.validator == lane_operator && s.msg.0 == dp_hash && s.msg.1 == lane_size
        });
        if !found_operator_signature {
            warn!(
                "Operator signature missing from sync reply entry {lane_operator} for DP {dp_hash}"
            );
        }

        let dp_size = data_proposal.estimate_size() as u64;
        let dp_parent_hash = data_proposal.parent_data_proposal_hash.clone();
        let metadata = LaneEntryMetadata {
            parent_data_proposal_hash: data_proposal.parent_data_proposal_hash.clone(),
            cumul_size: lane_size,
            signatures,
            cached_poda: None,
        };
        lanes.put_no_verification(lane_id.clone(), (metadata, data_proposal))?;
        debug!(
            "Filled hole {} for lane {} in BUC (slot: {})",
            dp_hash, lane_id, buc.ccp.consensus_proposal.slot
        );

        // Compute the next hole top
        let next_top = match &dp_parent_hash {
            DataProposalParent::DP(parent_hash) => {
                let Some(new_lane_size) = lane_size.0.checked_sub(dp_size) else {
                    bail!(
                        "Lane size underflow while filling hole for lane {lane_id}: current {:?}, dp_size {}",
                        lane_size,
                        dp_size
                    );
                };
                // If we reached the 'from' cut, remove the hole
                if buc.from.as_ref().is_some_and(|from_cut| {
                    from_cut.iter().any(|(from_lane_id, from_dp_hash, _, _)| {
                        from_lane_id == lane_id && from_dp_hash == parent_hash
                    })
                }) {
                    debug!("Hole in lane {} is filled.", lane_id);
                    None
                } else {
                    debug!("Next hole in lane {}: {}", lane_id, parent_hash);
                    Some((parent_hash.clone(), LaneBytesSize(new_lane_size)))
                }
            }
            DataProposalParent::LaneRoot(_) => None,
        };

        // Inform dissemination we can stop requesting this DP.
        Self::send_dissemination_event_over(
            bus,
            DisseminationEvent::DpStored {
                lane_id: lane_id.clone(),
                data_proposal_hash: dp_hash.clone(),
                cumul_size: lane_size,
            },
        )?;
        Self::send_dissemination_event_over(
            bus,
            DisseminationEvent::SyncRequestProgress {
                lane_id: lane_id.clone(),
                old_to: dp_hash.clone(),
                new_to: next_top.as_ref().map(|(dp, ..)| dp.clone()),
            },
        )?;

        Ok(next_top)
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
            "Materializing holes for BUC(slot: {}, parent: {})",
            buc.ccp.consensus_proposal.slot,
            buc.ccp.consensus_proposal.parent_hash
        );
        debug!(
            "Materializing holes for Block Under Construction {} from parent hash {}",
            buc.ccp.consensus_proposal.slot, buc.ccp.consensus_proposal.parent_hash
        );

        let mut any_missing = false;

        for (lane_id, to_hash, cumul_size, _) in buc.ccp.consensus_proposal.cut.iter() {
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
                        buc.holes_tops.insert(lane_id.clone(), (hash, *cumul_size));
                        any_missing = true;
                        break;
                    }
                    Err(_) => {
                        debug!(
                            "Lane {lane_id} missing data between {:?} and {:?}",
                            from_hash, to_hash
                        );
                        // Record the to_hash as hole top
                        buc.holes_tops
                            .insert(lane_id.clone(), (to_hash.clone(), *cumul_size));
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
    ) -> Result<Vec<(LaneId, Vec<ArcBorsh<DataProposal>>)>> {
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
                    Ok(EntryOrMissingHash::Entry(_, dp)) => {
                        dps.push(ArcBorsh::new(dp));
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
        // Resolve holes from buffered or stored data before materializing/requests.
        self.resolve_all_holes(buc)?;

        // First time, materialize holes (this is done somewhat async as it can be slow,
        // but we might want to refactor this in the future.)
        if !buc.holes_materialized && self.materialize_holes_for_signed_block(buc).await? {
            self.resolve_all_holes(buc)?;
            // Now send sync requests for remaining holes.
            // For simplicity I'll just re-loop here.
            for (lane_id, (to_hash, _)) in &buc.holes_tops {
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

        let cut_lanes = block_data.len();
        let lanes_with_new_data = block_data.iter().filter(|(_, dps)| !dps.is_empty()).count();
        let total_new_data_proposals: usize = block_data.iter().map(|(_, dps)| dps.len()).sum();

        debug!(
            "🚧 Built signed block for slot {} with {} cut lanes, {} lanes with new data, {} new data proposals",
            buc.ccp.consensus_proposal.slot,
            cut_lanes,
            lanes_with_new_data,
            total_new_data_proposals
        );

        // Delete stored proofs for all committed DataProposals - we don't need them anymore
        for (lane_id, dps) in &mut block_data {
            for dp in dps {
                self.lanes.delete_proofs(lane_id, &dp.hashed())?;
            }
        }

        self.bus
            .send(MempoolBlockEvent::BuiltSignedBlock(ArcSignedBlock {
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
        enum LaneCleanupAction {
            // The committed cut is already on our current ancestry; keep local state as-is.
            Noop,
            // We walked back to the previous cut first, so local descendants may belong to a fork.
            CleanToPreviousCut,
            // We could not connect the tip to either cut within the bounded walk; retip conservatively.
            ForceRetip,
        }

        for (lane_id, data_proposal_hash, cumul_size, _) in cut.iter() {
            let previous_committed_dp_hash = previous_cut
                .as_ref()
                .and_then(|cut| cut.iter().find(|(v, _, _, _)| v == lane_id))
                .map(|(_, h, _, _)| h);
            let mut cursor = self.lanes.get_lane_hash_tip(lane_id);
            let mut action = LaneCleanupAction::ForceRetip;

            for scan_depth in 0..=Self::CLEAN_AND_UPDATE_SCAN_LIMIT {
                let Some(current_hash) = cursor else {
                    break;
                };

                if current_hash == *data_proposal_hash {
                    action = LaneCleanupAction::Noop;
                    break;
                }

                if previous_committed_dp_hash == Some(&current_hash) {
                    action = LaneCleanupAction::CleanToPreviousCut;
                    break;
                }

                let Ok(Some(metadata)) = self.lanes.get_metadata_by_hash(lane_id, &current_hash)
                else {
                    break;
                };

                cursor = match metadata.parent_data_proposal_hash {
                    DataProposalParent::DP(parent_hash) => Some(parent_hash),
                    DataProposalParent::LaneRoot(_) => None,
                };

                if scan_depth == Self::CLEAN_AND_UPDATE_SCAN_LIMIT {
                    warn!(
                        lane_id = %lane_id,
                        scan_limit = Self::CLEAN_AND_UPDATE_SCAN_LIMIT,
                        previous_committed_dp_hash = ?previous_committed_dp_hash,
                        new_committed_dp_hash = %data_proposal_hash,
                        "Exceeded clean-and-update scan limit; force-retipping lane"
                    );
                }
            }

            match action {
                LaneCleanupAction::Noop => {}
                LaneCleanupAction::CleanToPreviousCut => {
                    debug!(
                        lane_id = %lane_id,
                        previous_committed_dp_hash = ?previous_committed_dp_hash,
                        new_committed_dp_hash = %data_proposal_hash,
                        "Retipping lane to committed cut before cleaning back to previous cut"
                    );
                    let mut cursor = self.lanes.get_lane_hash_tip(lane_id);
                    self.lanes.update_lane_tip(
                        lane_id.clone(),
                        data_proposal_hash.clone(),
                        *cumul_size,
                    );

                    while cursor.as_ref() != previous_committed_dp_hash {
                        let Some(removed_hash) = cursor.clone() else {
                            break;
                        };
                        let Ok(Some((_, (removed_metadata, removed_dp)))) =
                            self.lanes.remove_by_hash(lane_id, &removed_hash)
                        else {
                            // if there's an error, stop removing and move on to the next lane.
                            break;
                        };
                        cursor = match removed_metadata.parent_data_proposal_hash.clone() {
                            DataProposalParent::DP(parent_hash) => Some(parent_hash),
                            DataProposalParent::LaneRoot(_) => None,
                        };
                        self.buffered_entries
                            .entry(lane_id.clone())
                            .or_default()
                            .insert(removed_hash, (removed_metadata.signatures, removed_dp));
                    }
                    self.cached_dp_votes
                        .retain(|(cached_lane_id, _), _| cached_lane_id != lane_id);
                }
                LaneCleanupAction::ForceRetip => {
                    info!(
                        lane_id = %lane_id,
                        previous_committed_dp_hash = ?previous_committed_dp_hash,
                        new_committed_dp_hash = %data_proposal_hash,
                        "Force-retipping lane to committed cut"
                    );
                    self.lanes.update_lane_tip(
                        lane_id.clone(),
                        data_proposal_hash.clone(),
                        *cumul_size,
                    );
                    self.cached_dp_votes
                        .retain(|(cached_lane_id, _), _| cached_lane_id != lane_id);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod test {
    use hyli_crypto::BlstCrypto;
    use staking::state::Staking;
    use utils::TimestampMs;

    use crate::mempool::MempoolNetMessage;
    use crate::tests::autobahn_testing_macros::assert_chanmsg_matches;

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
                    vec![(LaneId::new(key.clone()), vec![dp_orig.into()])]
                );
            }
        );

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn proofs_deleted_after_commit() -> Result<()> {
        use crate::model::{
            BlobProofOutput, ContractName, HyliOutput, ProgramId, ProofData, Transaction,
            TransactionData, VerifiedProofTransaction, Verifier,
        };

        let mut ctx = MempoolTestCtx::new("mempool").await;

        // Create a DP with a VerifiedProof tx containing an inlined proof
        let proof = ProofData(vec![1, 2, 3, 4, 5]);
        let proof_hash = proof.hashed();
        let vpt = VerifiedProofTransaction {
            contract_name: ContractName::new("cleanup-proof"),
            program_id: ProgramId(vec![]),
            verifier: Verifier("test".into()),
            proof: Some(proof.clone()),
            proof_hash: proof_hash.clone(),
            proof_size: proof.0.len(),
            proven_blobs: vec![BlobProofOutput {
                original_proof_hash: proof_hash,
                blob_tx_hash: b"blob-tx".into(),
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
                    vec![(
                        LaneId::new(key.clone()),
                        vec![dp_orig.into(), dp_orig2.into(), dp_orig3.into()]
                    )]
                );
            }
        );

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn signed_block_start_building_later() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;

        let dp2_size = LaneBytesSize(20);
        let dp2_hash: DataProposalHash = b"dp2".into();
        let dp5_size = LaneBytesSize(50);
        let dp5_hash: DataProposalHash = b"dp5".into();
        let dp6_size = LaneBytesSize(60);
        let dp6_hash: DataProposalHash = b"dp6".into();

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
        let dp1 = DataProposal::new_root(lane_id1.clone(), vec![]);
        let dp1_hash = dp1.hashed();
        ctx.mempool
            .lanes
            .store_data_proposal(&crypto1, &lane_id1, dp1.clone())?;

        let dp2 = DataProposal::new(dp1_hash.clone(), vec![]);
        let dp2_hash = dp2.hashed();
        ctx.mempool
            .lanes
            .store_data_proposal(&crypto1, &lane_id1, dp2.clone())?;

        // Create a chain of 2 DataProposals in lane 2
        let dp3 = DataProposal::new_root(lane_id2.clone(), vec![]);
        let dp3_hash = dp3.hashed();
        ctx2.mempool
            .lanes
            .store_data_proposal(&crypto2, &lane_id2, dp3.clone())?;

        let dp4 = DataProposal::new(dp3_hash.clone(), vec![]);
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
            parent_hash: b"test".into(),
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

        // Simulate hole being filled (normally done by sync replies).
        buc.holes_tops.clear();

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
                    (
                        LaneId::new(crypto1.validator_pubkey().clone()),
                        vec![dp1.into(), dp2.into()]
                    )
                );
                assert_eq!(
                    signed_block.data_proposals[1],
                    (
                        LaneId::new(crypto2.validator_pubkey().clone()),
                        vec![dp3.into(), dp4.into()]
                    )
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
        let dp1 = DataProposal::new_root(lane_id.clone(), vec![]);
        let dp1_hash = dp1.hashed();
        ctx.mempool
            .lanes
            .store_data_proposal(&crypto, &lane_id, dp1)?;

        let dp2 = DataProposal::new(dp1_hash.clone(), vec![]);
        let dp2_hash = dp2.hashed();
        ctx.mempool
            .lanes
            .store_data_proposal(&crypto, &lane_id, dp2)?;

        let dp3 = DataProposal::new(dp2_hash.clone(), vec![]);
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
            parent_hash: b"test".into(),
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

    #[test_log::test(tokio::test)]
    async fn test_committed_child_without_parent_from_known_tip_requests_sync() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;
        let peer_crypto = BlstCrypto::new("peer").unwrap();
        let lane_id = LaneId::new(peer_crypto.validator_pubkey().clone());
        ctx.add_trusted_validator(peer_crypto.validator_pubkey())
            .await;

        // Local tip is A.
        let dp_a = DataProposal::new_root(lane_id.clone(), vec![]);
        let (dp_a_hash, dp_a_cumul_size) =
            ctx.mempool
                .lanes
                .store_data_proposal(&peer_crypto, &lane_id, dp_a)?;

        // Receive C whose parent is B (missing locally).
        let dp_b = DataProposal::new(dp_a_hash.clone(), vec![]);
        let dp_c = DataProposal::new(dp_b.hashed(), vec![]);
        let dp_c_size = LaneBytesSize(dp_c.estimate_size() as u64);
        let dp_c_hash = dp_c.hashed();
        let vote_c = peer_crypto.sign((dp_c_hash.clone(), dp_c_size))?;
        ctx.mempool
            .on_hashed_data_proposal(&lane_id, dp_c, vote_c)
            .await?;

        // C is not yet storable because B is missing.
        assert!(ctx
            .mempool
            .lanes
            .get_dp_by_hash(&lane_id, &dp_c_hash)?
            .is_none());

        // C gets committed while B is still missing.
        let mut buc = BlockUnderConstruction {
            from: Some(vec![(
                lane_id.clone(),
                dp_a_hash.clone(),
                dp_a_cumul_size,
                AggregateSignature::default(),
            )]),
            ccp: CommittedConsensusProposal {
                consensus_proposal: ConsensusProposal {
                    slot: 1,
                    cut: vec![(
                        lane_id.clone(),
                        dp_c_hash.clone(),
                        LaneBytesSize(dp_a_cumul_size.0 + dp_c_size.0),
                        AggregateSignature::default(),
                    )],
                    staking_actions: vec![],
                    timestamp: TimestampMs(0),
                    parent_hash: b"test".into(),
                },
                staking: Staking::default(),
                certificate: AggregateSignature::default(),
            },
            holes_tops: HashMap::new(),
            holes_materialized: false,
        };

        let result = ctx.mempool.build_signed_block_and_emit(&mut buc).await;
        assert!(result.is_err());
        assert_eq!(
            buc.holes_tops.get(&lane_id).map(|(h, _)| h.clone()),
            Some(dp_c_hash.clone())
        );

        ctx.process_sync().await?;

        match ctx
            .assert_send(peer_crypto.validator_pubkey(), "SyncRequest")
            .await
            .msg
        {
            MempoolNetMessage::SyncRequest(req_lane_id, from, to) => {
                assert_eq!(req_lane_id, lane_id);
                assert_eq!(from, Some(dp_a_hash));
                assert_eq!(to, Some(dp_c_hash));
            }
            _ => panic!("Expected SyncRequest message"),
        }

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_lane_tip_updated_before_try_send_with_buffered_child() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;
        let peer_crypto = BlstCrypto::new("peer-tip-order").unwrap();
        let lane_id = LaneId::new(peer_crypto.validator_pubkey().clone());
        ctx.add_trusted_validator(peer_crypto.validator_pubkey())
            .await;

        // A -> B -> C chain, but only A is stored; C is buffered (B missing).
        let dp_a = DataProposal::new_root(lane_id.clone(), vec![Transaction::default()]);
        let (dp_a_hash, dp_a_cumul_size) =
            ctx.mempool
                .lanes
                .store_data_proposal(&peer_crypto, &lane_id, dp_a)?;

        let dp_b = DataProposal::new(dp_a_hash.clone(), vec![Transaction::default()]);
        let dp_b_hash = dp_b.hashed();
        let dp_c = DataProposal::new(dp_b_hash.clone(), vec![Transaction::default()]);
        let dp_c_hash = dp_c.hashed();
        let dp_c_cumul_size = LaneBytesSize(
            dp_a_cumul_size.0 + dp_b.estimate_size() as u64 + dp_c.estimate_size() as u64,
        );

        // Receive C first -> buffered because parent B is unknown.
        let vote_c = peer_crypto.sign((dp_c_hash.clone(), dp_c_cumul_size))?;
        ctx.mempool
            .on_hashed_data_proposal(&lane_id, dp_c, vote_c)
            .await?;

        // Commit slot 1 at A, then slot 2 at C.
        ctx.process_cut_with_dp(
            peer_crypto.validator_pubkey(),
            &dp_a_hash,
            dp_a_cumul_size,
            1,
        )
        .await?;
        ctx.process_cut_with_dp(
            peer_crypto.validator_pubkey(),
            &dp_c_hash,
            dp_c_cumul_size,
            2,
        )
        .await?;

        // Non-regression: lane tip must be updated to C even if BUC still has a hole on B.
        assert_eq!(
            ctx.mempool.lanes.get_lane_hash_tip(&lane_id),
            Some(dp_c_hash)
        );

        // C was insta-filled from buffer; remaining hole should now point to B.
        let buc = ctx
            .mempool
            .inner
            .blocks_under_contruction
            .front()
            .expect("expected one pending BUC");
        assert_eq!(
            buc.holes_tops.get(&lane_id).map(|(h, _)| h.clone()),
            Some(dp_b_hash)
        );

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_clean_and_update_lanes_buffers_cleaned_fork_entries() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;
        let peer_crypto = BlstCrypto::new("peer-clean-fork").unwrap();
        let lane_id = LaneId::new(peer_crypto.validator_pubkey().clone());

        let dp_a = DataProposal::new_root(lane_id.clone(), vec![Transaction::default()]);
        let (dp_a_hash, dp_a_size) =
            ctx.mempool
                .lanes
                .store_data_proposal(&peer_crypto, &lane_id, dp_a)?;

        let dp_b = DataProposal::new(dp_a_hash.clone(), vec![Transaction::default()]);
        let (dp_b_hash, dp_b_size) =
            ctx.mempool
                .lanes
                .store_data_proposal(&peer_crypto, &lane_id, dp_b)?;

        let dp_c = DataProposal::new(dp_b_hash.clone(), vec![Transaction::default()]);
        let (dp_c_hash, _dp_c_size) =
            ctx.mempool
                .lanes
                .store_data_proposal(&peer_crypto, &lane_id, dp_c)?;

        let committed_hash: DataProposalHash = b"committed-fork-tip".into();
        let committed_size = LaneBytesSize(dp_b_size.0 + 10);
        let cut = vec![(
            lane_id.clone(),
            committed_hash.clone(),
            committed_size,
            AggregateSignature::default(),
        )];
        let previous_cut = Some(vec![(
            lane_id.clone(),
            dp_a_hash.clone(),
            dp_a_size,
            AggregateSignature::default(),
        )]);

        ctx.mempool.clean_and_update_lanes(&cut, &previous_cut)?;

        assert_eq!(
            ctx.mempool.lanes.get_lane_hash_tip(&lane_id),
            Some(committed_hash)
        );
        assert!(ctx.mempool.lanes.contains(&lane_id, &dp_a_hash));
        assert!(!ctx.mempool.lanes.contains(&lane_id, &dp_b_hash));
        assert!(!ctx.mempool.lanes.contains(&lane_id, &dp_c_hash));

        let buffered = ctx
            .mempool
            .buffered_entries
            .get(&lane_id)
            .expect("expected buffered cleaned entries");
        assert!(buffered.contains_key(&dp_b_hash));
        assert!(buffered.contains_key(&dp_c_hash));

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_clean_and_update_lanes_retips_without_cleaning_after_scan_limit() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;
        let peer_crypto = BlstCrypto::new("peer-clean-limit").unwrap();
        let lane_id = LaneId::new(peer_crypto.validator_pubkey().clone());

        let first_dp = DataProposal::new_root(lane_id.clone(), vec![Transaction::default()]);
        let (first_hash, first_size) =
            ctx.mempool
                .lanes
                .store_data_proposal(&peer_crypto, &lane_id, first_dp)?;

        let mut parent_hash = first_hash.clone();
        let mut current_size = first_size;
        let mut latest_hash = first_hash.clone();

        for _ in 0..101 {
            let dp = DataProposal::new(parent_hash.clone(), vec![Transaction::default()]);
            let (dp_hash, dp_size) =
                ctx.mempool
                    .lanes
                    .store_data_proposal(&peer_crypto, &lane_id, dp)?;
            parent_hash = dp_hash.clone();
            current_size = dp_size;
            latest_hash = dp_hash;
        }

        let committed_hash: DataProposalHash = b"committed-hard-limit".into();
        let committed_size = LaneBytesSize(current_size.0 + 10);
        let cut = vec![(
            lane_id.clone(),
            committed_hash.clone(),
            committed_size,
            AggregateSignature::default(),
        )];
        let previous_cut = Some(vec![(
            lane_id.clone(),
            first_hash.clone(),
            first_size,
            AggregateSignature::default(),
        )]);

        ctx.mempool.clean_and_update_lanes(&cut, &previous_cut)?;

        assert_eq!(
            ctx.mempool.lanes.get_lane_hash_tip(&lane_id),
            Some(committed_hash)
        );
        assert!(ctx.mempool.lanes.contains(&lane_id, &latest_hash));
        assert!(ctx
            .mempool
            .buffered_entries
            .get(&lane_id)
            .is_none_or(|x| x.is_empty()));

        Ok(())
    }
}

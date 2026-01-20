//! Logic for processing the API inbound TXs in the mempool.

use crate::{bus::BusClientSender, model::*, utils::serialize::BorshableIndexMap};

use anyhow::{bail, Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use client_sdk::tcp_client::TcpServerMessage;
use hyli_turmoil_shims::collections::HashMap;
use tracing::{debug, info, trace};

use super::verifiers::{verify_proof, verify_recursive_proof};
use super::DisseminationEvent;
#[cfg(test)]
use super::MempoolNetMessage;
use super::{api::RestApiMessage, storage::Storage};
use indexmap::IndexMap;
use std::{collections::HashSet, sync::Arc};
use tokio::task::Id as TaskId;
use tokio::task::JoinSet;

const MAX_DP_SIZE: usize = 40_000_000; // About 40 MB

#[derive(Default, BorshSerialize, BorshDeserialize)]
pub(super) struct OwnDataProposalPreparation {
    #[borsh(skip)]
    tasks: JoinSet<(LaneId, DataProposalHash)>,
    #[borsh(skip)]
    lanes: HashSet<LaneId>,
    prepared: HashMap<LaneId, super::ArcBorsh<DataProposal>>,
    #[borsh(skip)]
    task_ids: HashMap<TaskId, LaneId>,
}

impl OwnDataProposalPreparation {
    pub(super) fn is_lane_in_flight(&self, lane_id: &LaneId) -> bool {
        self.lanes.contains(lane_id)
    }

    pub(super) fn spawn_on(
        &mut self,
        lane_id: LaneId,
        data_proposal: Arc<DataProposal>,
        handle: &tokio::runtime::Handle,
    ) {
        self.lanes.insert(lane_id.clone());
        self.prepared.insert(
            lane_id.clone(),
            super::ArcBorsh::new(Arc::clone(&data_proposal)),
        );
        let lane_id_clone = lane_id.clone();
        let task = self.tasks.spawn_on(
            async move { (lane_id_clone, data_proposal.hashed()) },
            handle,
        );
        self.task_ids.insert(task.id(), lane_id);
    }

    pub(super) fn take_failed_by_task_id(
        &mut self,
        task_id: TaskId,
    ) -> Option<(LaneId, Arc<DataProposal>)> {
        let lane_id = self.task_ids.remove(&task_id)?;
        self.lanes.remove(&lane_id);
        let data_proposal = self.prepared.remove(&lane_id)?;
        Some((lane_id, data_proposal.arc()))
    }

    pub(super) fn take_prepared_by_lane(&mut self, lane_id: &LaneId) -> Option<Arc<DataProposal>> {
        self.lanes.remove(lane_id);
        self.task_ids
            .retain(|_, stored_lane| stored_lane != lane_id);
        self.prepared.remove(lane_id).map(|dp| dp.arc())
    }

    pub(super) async fn join_next(
        &mut self,
    ) -> Option<Result<(LaneId, DataProposalHash), tokio::task::JoinError>> {
        self.tasks.join_next().await
    }

    pub(super) fn restore_tasks(&mut self, handle: &tokio::runtime::Handle) {
        for (lane_id, data_proposal) in self.prepared.iter() {
            if self.lanes.contains(lane_id) {
                continue;
            }
            self.lanes.insert(lane_id.clone());
            let lane_id_clone = lane_id.clone();
            let data_proposal = data_proposal.arc();
            let task = self.tasks.spawn_on(
                async move { (lane_id_clone, data_proposal.hashed()) },
                handle,
            );
            self.task_ids.insert(task.id(), lane_id.clone());
        }
    }
}

impl super::Mempool {
    fn get_last_data_prop_hash_in_own_lane(&self, lane_id: &LaneId) -> Option<DataProposalHash> {
        self.lanes.get_lane_hash_tip(lane_id)
    }

    #[cfg(test)]
    pub(super) fn own_lane_id(&self) -> LaneId {
        self.lane_id_for_suffix(self.default_blob_suffix())
    }

    fn default_blob_suffix(&self) -> &str {
        if self.conf.own_lanes.default_blob_suffix.is_empty() {
            LaneId::DEFAULT_SUFFIX
        } else {
            self.conf.own_lanes.default_blob_suffix.as_str()
        }
    }

    fn default_proof_suffix(&self) -> &str {
        if self.conf.own_lanes.default_proof_suffix.is_empty() {
            LaneId::DEFAULT_SUFFIX
        } else {
            self.conf.own_lanes.default_proof_suffix.as_str()
        }
    }

    fn is_configured_suffix(&self, suffix: &str) -> bool {
        !suffix.is_empty()
            && (self
                .conf
                .own_lanes
                .suffixes
                .iter()
                .any(|known| known == suffix)
                || suffix == self.default_blob_suffix()
                || suffix == self.default_proof_suffix())
    }

    fn lane_id_for_suffix(&self, suffix: &str) -> LaneId {
        LaneId::with_suffix(self.crypto.validator_pubkey().clone(), suffix.to_string())
    }

    fn resolve_lane_suffix_for_tx(
        &self,
        tx: &Transaction,
        lane_suffix: Option<&LaneSuffix>,
    ) -> LaneSuffix {
        let suffix = match lane_suffix {
            Some(suffix) => suffix.as_str(),
            None => match tx.transaction_data {
                TransactionData::Blob(_) => self.default_blob_suffix(),
                TransactionData::Proof(_) | TransactionData::VerifiedProof(_) => {
                    self.default_proof_suffix()
                }
            },
        };

        suffix.to_string()
    }

    fn lane_id_for_tx(&self, _tx: &Transaction, lane_suffix: &LaneSuffix) -> Option<LaneId> {
        let suffix = lane_suffix.as_str();
        if self.is_configured_suffix(suffix) {
            Some(self.lane_id_for_suffix(suffix))
        } else {
            None
        }
    }

    fn pending_txs_total(&self) -> usize {
        self.waiting_dissemination_txs
            .values()
            .map(|txs| txs.len())
            .sum()
    }

    pub(super) fn prepare_new_data_proposal(&mut self) -> Result<bool> {
        if !self.ready_to_create_dps {
            trace!("Skipping own-lane DP creation until first CCP is received");
            return Ok(false);
        }
        trace!("🐣 Prepare new owned data proposal");
        let mut started = false;
        let lane_ids: Vec<LaneId> = self.waiting_dissemination_txs.keys().cloned().collect();

        for lane_id in lane_ids {
            if self
                .inner
                .own_data_proposal_in_preparation
                .is_lane_in_flight(&lane_id)
            {
                continue;
            }
            let Some(dp) = self.init_dp_preparation_if_pending(&lane_id)? else {
                continue;
            };

            let data_proposal = Arc::new(dp);
            let handle = self.inner.long_tasks_runtime.handle();
            self.inner.own_data_proposal_in_preparation.spawn_on(
                lane_id.clone(),
                data_proposal,
                &handle,
            );
            started = true;
        }

        Ok(started)
    }

    /// In LaneManager mode we don't receive CCP events, so reconcile mempool state from
    /// NodeState-delivered blocks instead. On the first block after startup, we reconcile lane
    /// tips with the block's cut to avoid building own-lane DPs on stale tips.
    pub(super) fn on_lane_manager_new_block(&mut self, block: &NodeStateBlock) -> Result<()> {
        if !self.ready_to_create_dps {
            let cut = block.signed_block.consensus_proposal.cut.clone();
            self.clean_and_update_lanes(&cut, &None)?;
            self.ready_to_create_dps = true;
        }
        Ok(())
    }

    pub(super) async fn handle_own_data_proposal_preparation(
        &mut self,
        result: Result<(LaneId, DataProposalHash), tokio::task::JoinError>,
    ) -> Result<()> {
        // Requeue TXs if data proposal preparation fails to avoid losing work.
        match result {
            Ok((lane_id, _data_proposal_hash)) => {
                self.resume_new_data_proposal(lane_id).await?;
            }
            Err(err) => {
                tracing::warn!("Data proposal preparation task failed: {:?}", err);
                if let Some((lane_id, data_proposal)) = self
                    .inner
                    .own_data_proposal_in_preparation
                    .take_failed_by_task_id(err.id())
                {
                    self.restore_prepared_txs(lane_id, data_proposal)?;
                } else {
                    tracing::warn!("Failed to map data proposal preparation task to lane");
                }
            }
        }
        Ok(())
    }

    pub(super) async fn resume_new_data_proposal(&mut self, lane_id: LaneId) -> Result<()> {
        trace!("🐣 Create new data proposal");

        let Some(data_proposal) = self
            .inner
            .own_data_proposal_in_preparation
            .take_prepared_by_lane(&lane_id)
        else {
            bail!("Missing prepared data proposal for lane {}", lane_id);
        };
        #[allow(clippy::expect_used)]
        let data_proposal = Arc::try_unwrap(data_proposal)
            .expect("Prepared data proposal should be uniquely owned after task completion");
        self.register_new_data_proposal(lane_id, data_proposal)
    }

    /// Inits DataProposal preparation if there are pending transactions
    fn init_dp_preparation_if_pending(&mut self, lane_id: &LaneId) -> Result<Option<DataProposal>> {
        self.metrics.snapshot_pending_tx(self.pending_txs_total());
        let parent_hash = self.get_last_data_prop_hash_in_own_lane(lane_id);
        let parent = match parent_hash {
            Some(hash) => DataProposalParent::DP(hash),
            None => DataProposalParent::LaneRoot(lane_id.clone()),
        };
        let parent_hash_for_status = parent.as_tx_parent_hash();

        let (collected_txs, cumulative_size, remaining_len) = {
            let Some(waiting) = self.waiting_dissemination_txs.get_mut(lane_id) else {
                return Ok(None);
            };
            if waiting.is_empty() {
                return Ok(None);
            }

            let mut cumulative_size = 0;
            let mut current_idx = 0;
            while cumulative_size < MAX_DP_SIZE && current_idx < waiting.len() {
                if let Some((_tx_hash, tx)) = waiting.get_index(current_idx) {
                    cumulative_size += tx.estimate_size();
                    current_idx += 1;
                }
            }
            let collected_txs: Vec<Transaction> = waiting
                .drain(0..current_idx)
                .map(|(_tx_hash, tx)| tx)
                .collect();
            let remaining_len = waiting.len();

            (collected_txs, cumulative_size, remaining_len)
        };

        let status_event = MempoolStatusEvent::WaitingDissemination {
            parent_data_proposal_hash: parent_hash_for_status,
            txs: collected_txs.clone(),
        };

        self.bus
            .send(status_event)
            .context("Sending Status event for TX")?;

        debug!(
            "🌝 Creating new data proposals with {} txs (est. size {}). {} tx remain.",
            collected_txs.len(),
            cumulative_size,
            remaining_len
        );

        // Create new data proposal
        let data_proposal = match parent {
            DataProposalParent::LaneRoot(lane_id) => DataProposal::new_root(lane_id, collected_txs),
            DataProposalParent::DP(parent_hash) => DataProposal::new(parent_hash, collected_txs),
        };

        Ok(Some(data_proposal))
    }

    /// Register and do effects locally on own lane with prepared data proposal
    fn register_new_data_proposal(
        &mut self,
        lane_id: LaneId,
        data_proposal: DataProposal,
    ) -> Result<()> {
        let validator_key = self.crypto.validator_pubkey().clone();
        debug!(
            "Creating new DataProposal in local lane ({}) with {} transactions (parent: {:?})",
            validator_key,
            data_proposal.txs.len(),
            data_proposal.parent_data_proposal_hash
        );

        // TODO: handle this differently
        let parent_data_proposal_hash = data_proposal.parent_data_proposal_hash.as_tx_parent_hash();

        let txs_metadatas = data_proposal
            .txs
            .iter()
            .map(|tx| tx.metadata(parent_data_proposal_hash.clone()))
            .collect();

        // Brittle logic: size-batching can skip some TXs, so their WaitingDissemination status
        // won't be updated (their TxId effectively changes). Listeners should update any TX whose
        // parent is this DP hash, not only those in txs_metadatas.
        self.bus
            .send(MempoolStatusEvent::DataProposalCreated {
                parent_data_proposal_hash,
                data_proposal_hash: data_proposal.hashed(),
                txs_metadatas,
            })
            .context("Sending MempoolStatusEvent DataProposalCreated")?;

        self.metrics.created_data_proposals.add(1, &[]);

        let (data_proposal_hash, cumul_size) =
            self.lanes
                .store_data_proposal(&self.crypto, &lane_id, data_proposal)?;

        self.send_dissemination_event(DisseminationEvent::NewDpCreated {
            lane_id: lane_id.clone(),
            data_proposal_hash: data_proposal_hash.clone(),
        })?;
        self.send_dissemination_event(DisseminationEvent::DpStored {
            lane_id,
            data_proposal_hash,
            cumul_size,
        })?;

        Ok(())
    }

    pub(super) fn restore_prepared_txs(
        &mut self,
        lane_id: LaneId,
        data_proposal: Arc<DataProposal>,
    ) -> Result<()> {
        let waiting_lane = self.waiting_dissemination_txs.entry(lane_id).or_default();
        let existing = std::mem::take(waiting_lane);
        let mut new_map = IndexMap::with_capacity(data_proposal.txs.len() + existing.len());
        for tx in data_proposal.txs.iter().cloned() {
            new_map.insert(tx.hashed(), tx);
        }
        for (tx_hash, tx) in existing.0 {
            new_map.insert(tx_hash, tx);
        }
        *waiting_lane = BorshableIndexMap(new_map);
        Ok(())
    }

    pub(super) fn handle_api_message(&mut self, command: RestApiMessage) -> Result<()> {
        match command {
            RestApiMessage::NewTx { tx, lane_suffix } => self.on_new_api_tx(tx, lane_suffix)?,
        }
        Ok(())
    }

    pub(super) fn handle_tcp_server_message(&mut self, command: TcpServerMessage) -> Result<()> {
        match command {
            TcpServerMessage::NewTx(tx) => self.on_new_api_tx(tx, None)?,
        }
        Ok(())
    }

    pub(super) fn on_new_api_tx(
        &mut self,
        tx: Transaction,
        lane_suffix: Option<LaneSuffix>,
    ) -> Result<()> {
        let lane_suffix = self.resolve_lane_suffix_for_tx(&tx, lane_suffix.as_ref());

        let Some(lane_id) = self.lane_id_for_tx(&tx, &lane_suffix) else {
            info!(
                "Dropping tx {} for unknown lane suffix {:?}",
                tx.hashed(),
                lane_suffix
            );
            return Ok(());
        };

        match &tx.transaction_data {
            // This is annoying to run in tests because we don't have the event loop setup, so go synchronous.
            #[cfg(test)]
            TransactionData::Blob(_) => self.on_new_tx(tx, lane_id)?,
            #[cfg(not(test))]
            TransactionData::Blob(_) => {
                let arc_tx = self.track_processing_tx(tx, lane_id);
                self.enqueue_processing_tx_hash(arc_tx)
            }
            TransactionData::Proof(_) => {
                let arc_tx = self.track_processing_tx(tx, lane_id);
                self.enqueue_processing_tx_proof(arc_tx)
            }
            TransactionData::VerifiedProof(_) => {
                bail!("VerifiedProofs cannot be sent over the API.")
            }
        }
        Ok(())
    }

    pub(super) fn on_new_tx(&mut self, tx: Transaction, lane_id: LaneId) -> Result<()> {
        // TODO: Verify fees ?

        let tx_type: &'static str = (&tx.transaction_data).into();
        trace!("Tx {} received in mempool", tx_type);

        match &tx.transaction_data {
            TransactionData::Blob(blob_tx) => {
                debug!("Got new blob tx {}", tx.hashed());
                if blob_tx.blobs.len() > 20 {
                    bail!(
                        "Blob transaction has too many blobs: {}",
                        blob_tx.blobs.len()
                    );
                }
            }
            TransactionData::Proof(_) => {
                unreachable!("Proof TXs are converted to VerifiedProof in on_new_api_tx");
            }
            TransactionData::VerifiedProof(proof_tx) => {
                debug!(
                    "Got verified proof tx {} for {}",
                    tx.hashed(),
                    proof_tx.contract_name
                );
            }
        }

        let tx_type: &'static str = (&tx.transaction_data).into();

        let tx_hash = tx.hashed();
        let waiting_lane = self.waiting_dissemination_txs.entry(lane_id).or_default();

        if waiting_lane.contains_key(&tx_hash) {
            debug!("Dropping duplicate tx {}", tx_hash);
            self.metrics.drop_api_tx(tx_type);
        } else {
            waiting_lane.insert(tx_hash.clone(), tx.clone());

            self.metrics.add_api_tx(tx_type);
        }

        self.metrics.snapshot_pending_tx(self.pending_txs_total());

        Ok(())
    }

    pub(super) fn enqueue_processing_tx_hash(&mut self, arc_tx: Arc<Transaction>) {
        let handle = self.inner.long_tasks_runtime.handle();
        self.inner.processing_txs.spawn_on(
            async move {
                arc_tx.hashed();
                Ok((*arc_tx).clone())
            },
            &handle,
        );
    }

    pub(super) fn enqueue_processing_tx_proof(&mut self, arc_tx: Arc<Transaction>) {
        let handle = self.inner.long_tasks_runtime.handle();
        self.inner.processing_txs.spawn_on(
            async move {
                arc_tx.hashed();
                let tx = Self::process_proof_tx((*arc_tx).clone())
                    .context("Processing proof tx in blocker")?;
                Ok(tx)
            },
            &handle,
        );
    }

    fn track_processing_tx(&mut self, tx: Transaction, lane_id: LaneId) -> Arc<Transaction> {
        let arc_tx = Arc::new(tx);
        self.inner
            .processing_txs_pending
            .push_back((super::ArcBorsh::new(Arc::clone(&arc_tx)), lane_id));
        arc_tx
    }

    pub(super) fn process_proof_tx(mut tx: Transaction) -> Result<Transaction> {
        let TransactionData::Proof(proof_transaction) = tx.transaction_data else {
            bail!("Can only process ProofTx");
        };

        let is_recursive = proof_transaction.contract_name.0 == "risc0-recursion";

        let (hyli_outputs, program_ids) = if is_recursive {
            let (program_ids, hyli_outputs) = verify_recursive_proof(
                &proof_transaction.proof,
                &proof_transaction.verifier,
                &proof_transaction.program_id,
            )
            .context("verify_rec_proof")?;
            (hyli_outputs, program_ids)
        } else {
            let hyli_outputs = verify_proof(
                &proof_transaction.proof,
                &proof_transaction.verifier,
                &proof_transaction.program_id,
            )
            .context("verify_proof")?;
            let len = hyli_outputs.len();
            (
                hyli_outputs,
                vec![proof_transaction.program_id.clone(); len],
            )
        };

        let tx_hashes = hyli_outputs
            .iter()
            .map(|ho| ho.tx_hash.clone())
            .collect::<Vec<_>>();

        std::iter::zip(&tx_hashes, std::iter::zip(&hyli_outputs, &program_ids)).for_each(
            |(blob_tx_hash, (hyli_output, program_id))| {
                debug!(
                    "Blob tx hash {} verified with hyli output {:?} and program id {}",
                    blob_tx_hash,
                    hyli_output,
                    hex::encode(&program_id.0)
                );
            },
        );

        tx.transaction_data = TransactionData::VerifiedProof(VerifiedProofTransaction {
            proof_hash: proof_transaction.proof.hashed(),
            proof_size: proof_transaction.estimate_size(),
            proof: Some(proof_transaction.proof),
            contract_name: proof_transaction.contract_name.clone(),
            program_id: proof_transaction.program_id.clone(),
            verifier: proof_transaction.verifier.clone(),
            is_recursive,
            proven_blobs: std::iter::zip(tx_hashes, std::iter::zip(hyli_outputs, program_ids))
                .map(
                    |(blob_tx_hash, (hyli_output, program_id))| BlobProofOutput {
                        original_proof_hash: ProofDataHash(b"todo?".to_vec()),
                        blob_tx_hash: blob_tx_hash.clone(),
                        hyli_output,
                        program_id,
                        // Should be the same verifier for all blobs in the proof
                        verifier: proof_transaction.verifier.clone(),
                    },
                )
                .collect(),
        });

        Ok(tx)
    }
}

#[allow(clippy::indexing_slicing)]
#[cfg(test)]
pub mod test {
    use core::panic;

    use super::*;
    use crate::{
        mempool::storage::LaneEntryMetadata, p2p::network::HeaderSigner,
        tests::autobahn_testing::assert_chanmsg_matches,
    };
    use anyhow::Result;
    use hyli_crypto::BlstCrypto;

    use crate::mempool::tests::*;

    #[test_log::test(tokio::test)]
    async fn test_single_mempool_receiving_new_txs() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;
        ctx.set_ready_to_create_dps(); // We want to process txs right away

        // Sending transaction to mempool as RestApiMessage
        let register_tx = make_register_contract_tx(ContractName::new("test1"));
        let register_tx_2 = make_register_contract_tx(ContractName::new("test2"));
        let register_tx_3 = make_register_contract_tx(ContractName::new("test3"));

        ctx.submit_tx(&register_tx);
        ctx.submit_tx(&register_tx);
        ctx.submit_tx(&register_tx_2);
        ctx.submit_tx(&register_tx);
        ctx.submit_tx(&register_tx_3);

        assert!(ctx.mempool_status_event_receiver.is_empty());

        ctx.timer_tick().await?;

        let dp_hash = ctx
            .mempool
            .lanes
            .get_lane_hash_tip(&ctx.own_lane())
            .unwrap();
        let dp = ctx
            .mempool
            .lanes
            .get_dp_by_hash(&ctx.own_lane(), &dp_hash)
            .unwrap()
            .unwrap();

        let actual_txs = vec![
            register_tx.clone(),
            register_tx_2.clone(),
            register_tx_3.clone(),
        ];

        assert_chanmsg_matches!(
            ctx.mempool_status_event_receiver,
            MempoolStatusEvent::WaitingDissemination { parent_data_proposal_hash, txs } => {
                assert_eq!(
                    parent_data_proposal_hash,
                    DataProposalHash::from(ctx.mempool.own_lane_id().to_string())
                );
                assert_eq!(txs, actual_txs);
            }
        );
        assert_eq!(dp.txs, actual_txs);

        // Assert that pending_tx has been flushed
        assert_eq!(ctx.mempool.pending_txs_total(), 0);

        assert_chanmsg_matches!(
            ctx.mempool_status_event_receiver,
            MempoolStatusEvent::DataProposalCreated { data_proposal_hash, txs_metadatas, .. } => {
                assert_eq!(data_proposal_hash, dp.hashed());
                assert_eq!(txs_metadatas.len(), dp.txs.len());
                assert_eq!(txs_metadatas.len(), 3);
            }
        );

        // Timer again with no txs
        ctx.timer_tick().await?;

        assert_eq!(
            ctx.mempool
                .get_last_data_prop_hash_in_own_lane(&ctx.own_lane())
                .unwrap(),
            dp.hashed()
        );

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_cache_poda_on_votes() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;
        let pubkey = (*ctx.mempool.crypto).clone();

        // Adding 4 other validators
        // Total voting_power = 500; f = 167 --> You need at least 2 signatures to cache PoDA
        let crypto2 = BlstCrypto::new("validator2").unwrap();
        let crypto3 = BlstCrypto::new("validator3").unwrap();
        let crypto4 = BlstCrypto::new("validator4").unwrap();
        let crypto5 = BlstCrypto::new("validator5").unwrap();
        ctx.setup_node(&[pubkey, crypto2.clone(), crypto3.clone(), crypto4, crypto5])
            .await;

        let register_tx = make_register_contract_tx(ContractName::new("test1"));
        let dp = ctx.create_data_proposal(None, &[register_tx]);
        ctx.process_new_data_proposal(dp)?;
        ctx.timer_tick().await?;

        let data_proposal = match ctx.assert_broadcast("DataProposal").await.msg {
            MempoolNetMessage::DataProposal(_, _, dp, _) => dp,
            _ => panic!("Expected DataProposal message"),
        };
        let size = LaneBytesSize(data_proposal.estimate_size() as u64);
        let lane_id = ctx.mempool.own_lane_id();

        // Simulate receiving votes from other validators
        let signed_msg2 =
            create_data_vote(&crypto2, lane_id.clone(), data_proposal.hashed(), size)?;
        let signed_msg3 =
            create_data_vote(&crypto3, lane_id.clone(), data_proposal.hashed(), size)?;
        // Clear event receiver
        while ctx.dissemination_event_receiver.try_recv().is_ok() {}
        ctx.mempool
            .handle_net_message(crypto2.sign_msg_with_header(signed_msg2)?)
            .await?;
        ctx.mempool
            .handle_net_message(crypto3.sign_msg_with_header(signed_msg3)?)
            .await?;

        // Assert that PoDA is cached once we cross f+1 votes
        let metadata = ctx
            .mempool
            .lanes
            .get_metadata_by_hash(&lane_id, &data_proposal.hashed())?
            .expect("metadata should exist");
        assert!(metadata.cached_poda.is_some());

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_broadcast_rehydrates_proofs() -> Result<()> {
        use crate::model::{
            BlobProofOutput, ContractName, HyliOutput, ProgramId, ProofData, Transaction,
            TransactionData, VerifiedProofTransaction, Verifier,
        };

        let mut ctx = MempoolTestCtx::new("mempool").await;

        // Bond self and another validator so broadcast happens
        let crypto2 = BlstCrypto::new("validator2").unwrap();
        let self_crypto = (*ctx.mempool.crypto).clone();
        ctx.setup_node(&[self_crypto, crypto2.clone()]).await;

        // Build DP with a VerifiedProof tx including the proof
        let proof = ProofData(vec![9, 9, 9, 9]);
        let proof_hash = proof.hashed();
        let vpt = VerifiedProofTransaction {
            contract_name: ContractName::new("rehydrate"),
            program_id: ProgramId(vec![]),
            verifier: Verifier("test".into()),
            proof: Some(proof.clone()),
            proof_hash: proof_hash.clone(),
            proof_size: proof.0.len(),
            proven_blobs: vec![BlobProofOutput {
                original_proof_hash: proof_hash,
                blob_tx_hash: crate::model::TxHash(b"blob-tx".to_vec()),
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

        // Store DP locally (will strip proofs and store them side-by-side)
        ctx.process_new_data_proposal(dp.clone())?;

        // Trigger dissemination flow
        ctx.timer_tick().await?;

        // We should broadcast a DataProposal with rehydrated proofs
        let broadcast = ctx.assert_broadcast("DataProposal").await;
        let dp_broadcast = match broadcast.msg {
            MempoolNetMessage::DataProposal(_, _, dp, _) => dp,
            _ => panic!("Expected DataProposal message"),
        };

        // The broadcasted DP must include the proof
        match &dp_broadcast.txs[0].transaction_data {
            TransactionData::VerifiedProof(v) => {
                assert!(v.proof.is_some());
                assert_eq!(v.proof.as_ref().unwrap().0, vec![9, 9, 9, 9]);
            }
            _ => panic!("expected VerifiedProof tx"),
        }

        // The stored DP should remain without proofs
        let lane = LaneId::new(ctx.validator_pubkey().clone());
        let (_, dp_hash) = ctx.last_lane_entry(&lane);
        let stored_dp = ctx
            .mempool
            .lanes
            .get_dp_by_hash(&lane, &dp_hash)
            .unwrap()
            .unwrap();
        match &stored_dp.txs[0].transaction_data {
            TransactionData::VerifiedProof(v) => assert!(v.proof.is_none()),
            _ => panic!("expected VerifiedProof tx"),
        }

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_receiving_data_proposal_vote_from_unexpected_validator() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;

        let data_proposal = ctx.create_data_proposal(
            None,
            &[make_register_contract_tx(ContractName::new("test1"))],
        );
        let size = LaneBytesSize(data_proposal.estimate_size() as u64);
        let lane_id = ctx.mempool.own_lane_id();

        let temp_crypto = BlstCrypto::new("temp_crypto").unwrap();
        let signed_msg =
            create_data_vote(&temp_crypto, lane_id.clone(), data_proposal.hashed(), size)?;
        ctx.mempool
            .handle_net_message(temp_crypto.sign_msg_with_header(signed_msg)?)
            .await
            .expect("vote should be buffered");

        let buffered = ctx
            .mempool
            .inner
            .buffered_votes
            .get(&lane_id)
            .and_then(|lane| lane.get(&data_proposal.hashed()))
            .map(|votes| votes.len())
            .unwrap_or_default();
        assert_eq!(buffered, 1);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_receiving_data_proposal_vote() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;

        // Store the DP locally.
        let register_tx = make_register_contract_tx(ContractName::new("test1"));
        let data_proposal = ctx.create_data_proposal(None, std::slice::from_ref(&register_tx));
        ctx.process_new_data_proposal(data_proposal.clone())?;

        // Then make another validator vote on it.
        let size = LaneBytesSize(data_proposal.estimate_size() as u64);
        let data_proposal_hash = data_proposal.hashed();
        let lane_id = ctx.mempool.own_lane_id();

        // Add new validator
        let crypto2 = BlstCrypto::new("2").unwrap();
        ctx.add_trusted_validator(crypto2.validator_pubkey()).await;

        let signed_msg = create_data_vote(&crypto2, lane_id.clone(), data_proposal_hash, size)?;

        ctx.mempool
            .handle_net_message(crypto2.sign_msg_with_header(signed_msg)?)
            .await
            .expect("should handle net message");

        // Assert that we added the vote to the signatures
        let ((LaneEntryMetadata { signatures, .. }, _), _) =
            ctx.last_lane_entry(&LaneId::new(ctx.validator_pubkey().clone()));

        assert_eq!(signatures.len(), 2);
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_receiving_vote_for_unknown_data_proposal() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;

        // Add new validator
        let crypto2 = BlstCrypto::new("2").unwrap();
        ctx.add_trusted_validator(crypto2.validator_pubkey()).await;

        let signed_msg = create_data_vote(
            &crypto2,
            ctx.mempool.own_lane_id(),
            DataProposalHash(b"non_existent".to_vec()),
            LaneBytesSize(0),
        )?;

        ctx.mempool
            .handle_net_message(crypto2.sign_msg_with_header(signed_msg)?)
            .await
            .expect("vote should be buffered");

        let buffered = ctx
            .mempool
            .inner
            .buffered_votes
            .get(&ctx.mempool.own_lane_id())
            .and_then(|lane| lane.get(&DataProposalHash(b"non_existent".to_vec())))
            .map(|votes| votes.len())
            .unwrap_or_default();
        assert_eq!(buffered, 1);
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_redisseminate_multiple_data_proposals_four_nodes() -> Result<()> {
        // Crée 4 mempools/nœuds
        let mut ctx1 = MempoolTestCtx::new("node1").await;
        let mut ctx2 = MempoolTestCtx::new("node2").await;
        let mut ctx3 = MempoolTestCtx::new("node3").await;
        let mut ctx4 = MempoolTestCtx::new("node4").await;

        ctx1.set_ready_to_create_dps(); // We want to process txs right away

        // Ajoute chaque nœud comme validateur des autres
        let all_pubkeys = vec![
            ctx1.validator_pubkey().clone(),
            ctx2.validator_pubkey().clone(),
            ctx3.validator_pubkey().clone(),
            ctx4.validator_pubkey().clone(),
        ];
        for ctx in [&mut ctx1, &mut ctx2, &mut ctx3, &mut ctx4] {
            for pk in &all_pubkeys {
                ctx.add_trusted_validator(pk).await;
            }
        }

        // Soumets deux transactions à ctx1 pour générer deux DPs
        let tx1 = make_register_contract_tx(ContractName::new("test1"));
        let tx2 = make_register_contract_tx(ContractName::new("test2"));
        ctx1.submit_tx(&tx1);
        ctx1.timer_tick().await?;
        ctx1.submit_tx(&tx2);
        ctx1.timer_tick().await?;

        let mut dps = vec![];
        for _ in 0..2 {
            match ctx1.assert_broadcast("DataProposal").await.msg {
                MempoolNetMessage::DataProposal(_, hash, dp, _) => dps.push((hash, dp)),
                _ => panic!("Expected DataProposal message"),
            }
        }

        assert!(dps.len() == 2, "Should have two DataProposals");
        assert_eq!(dps[0].1.txs.len(), 1);
        assert_eq!(dps[1].1.txs.len(), 1);
        let txs = dps
            .iter()
            .map(|(_, dp)| dp.txs[0].clone())
            .collect::<Vec<_>>();
        assert!(txs.contains(&tx1));
        assert!(txs.contains(&tx2));

        // Redisseminate the oldest pending DataProposal
        // TODO: implement this as more of an integration test?
        let (oldest_hash, _) = dps
            .iter()
            .find(|(_, dp)| dp.parent_data_proposal_hash.is_lane_root())
            .expect("oldest dp should exist");
        ctx1.maybe_disseminate_dp(&ctx1.own_lane(), oldest_hash)
            .expect("disseminate");

        // Récupère les deux DataProposals broadcastées par ctx1
        let mut dps = vec![];
        for _ in 0..1 {
            match ctx1.assert_broadcast("DataProposal").await.msg {
                MempoolNetMessage::DataProposal(_, hash, dp, _) => dps.push((hash, dp)),
                _ => panic!("Expected DataProposal message"),
            }
        }

        assert!(dps.len() == 1, "Should have the oldest DataProposal");
        assert_eq!(dps[0].1.txs.len(), 1);
        assert_eq!(dps[0].1.txs[0], tx1);

        Ok(())
    }
}

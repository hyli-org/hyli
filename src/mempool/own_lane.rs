//! Logic for processing the API inbound TXs in the mempool.

use crate::{bus::BusClientSender, model::*};

use anyhow::{bail, Context, Result};
use client_sdk::tcp_client::TcpServerMessage;
use tracing::{debug, trace};

use super::verifiers::{verify_proof, verify_recursive_proof};
#[cfg(test)]
use super::MempoolNetMessage;
use super::{api::RestApiMessage, storage::Storage};
use super::{DisseminationEvent, ValidatorDAG};

impl super::Mempool {
    fn get_last_data_prop_hash_in_own_lane(&self) -> Option<DataProposalHash> {
        self.lanes.get_lane_hash_tip(&self.own_lane_id())
    }

    pub(super) fn own_lane_id(&self) -> LaneId {
        LaneId(self.crypto.validator_pubkey().clone())
    }

    pub(super) fn on_data_vote(&mut self, lane_id: LaneId, vdag: ValidatorDAG) -> Result<()> {
        self.metrics.on_data_vote.add(1, &[]);

        let validator = vdag.signature.validator.clone();
        let data_proposal_hash = vdag.msg.0.clone();
        if lane_id != self.own_lane_id() {
            debug!(
                "Ignoring vote from {} for non-owned lane {} (dp {})",
                validator, lane_id, data_proposal_hash
            );
            return Ok(());
        }
        debug!(
            "Vote from {} on own lane {}, dp {}",
            validator, lane_id, data_proposal_hash
        );

        let signatures =
            self.lanes
                .add_signatures(&lane_id, &data_proposal_hash, std::iter::once(vdag))?;

        // Compute voting power of all signers to check if the DataProposal received enough votes
        let validators: Vec<ValidatorPublicKey> = signatures
            .iter()
            .map(|s| s.signature.validator.clone())
            .collect();
        let old_voting_power = self.staking.compute_voting_power(
            validators
                .iter()
                .filter(|v| *v != &validator)
                .cloned()
                .collect::<Vec<_>>()
                .as_slice(),
        );
        let new_voting_power = self.staking.compute_voting_power(validators.as_slice());
        let f = self.staking.compute_f();
        // Only send the message if voting power exceeds f, 2 * f or is exactly 3 * f + 1
        // This garentees that the message is sent only once per threshold
        if old_voting_power < f && new_voting_power >= f
            || old_voting_power < 2 * f && new_voting_power >= 2 * f
            || new_voting_power > 3 * f
        {
            self.send_dissemination_event(DisseminationEvent::PoDAReady {
                lane_id,
                data_proposal_hash,
                signatures,
            })?;
        }
        Ok(())
    }

    pub(super) fn prepare_new_data_proposal(&mut self) -> Result<bool> {
        trace!("🐣 Prepare new owned data proposal");
        let Some(dp) = self.init_dp_preparation_if_pending()? else {
            return Ok(false);
        };

        let handle = self.inner.long_tasks_runtime.handle();

        self.inner
            .own_data_proposal_in_preparation
            .spawn_on(async move { (dp.hashed(), dp) }, handle);

        Ok(true)
    }

    pub(super) async fn resume_new_data_proposal(
        &mut self,
        data_proposal: DataProposal,
    ) -> Result<()> {
        trace!("🐣 Create new data proposal");

        self.register_new_data_proposal(data_proposal)?;
        Ok(())
    }

    /// Inits DataProposal preparation if there are pending transactions
    fn init_dp_preparation_if_pending(&mut self) -> Result<Option<DataProposal>> {
        self.metrics
            .snapshot_pending_tx(self.waiting_dissemination_txs.len());
        if self.waiting_dissemination_txs.is_empty()
            || !self.own_data_proposal_in_preparation.is_empty()
        {
            return Ok(None);
        }

        let mut cumulative_size = 0;
        let mut current_idx = 0;
        while cumulative_size < 40_000_000 && current_idx < self.waiting_dissemination_txs.len() {
            if let Some((_tx_hash, tx)) = self.waiting_dissemination_txs.get_index(current_idx) {
                cumulative_size += tx.estimate_size();
                current_idx += 1;
            }
        }
        let collected_txs: Vec<Transaction> = self
            .waiting_dissemination_txs
            .drain(0..current_idx)
            .map(|(_tx_hash, tx)| tx)
            .collect();

        let status_event = MempoolStatusEvent::WaitingDissemination {
            parent_data_proposal_hash: self
                .get_last_data_prop_hash_in_own_lane()
                .unwrap_or(DataProposalHash(self.crypto.validator_pubkey().to_string())),
            txs: collected_txs.clone(),
        };

        self.bus
            .send(status_event)
            .context("Sending Status event for TX")?;

        debug!(
            "🌝 Creating new data proposals with {} txs (est. size {}). {} tx remain.",
            collected_txs.len(),
            cumulative_size,
            self.waiting_dissemination_txs.len()
        );

        // Create new data proposal
        let data_proposal =
            DataProposal::new(self.get_last_data_prop_hash_in_own_lane(), collected_txs);

        Ok(Some(data_proposal))
    }

    /// Register and do effects locally on own lane with prepared data proposal
    fn register_new_data_proposal(&mut self, data_proposal: DataProposal) -> Result<()> {
        let validator_key = self.crypto.validator_pubkey().clone();
        debug!(
            "Creating new DataProposal in local lane ({}) with {} transactions (parent: {:?})",
            validator_key,
            data_proposal.txs.len(),
            data_proposal.parent_data_proposal_hash
        );

        // TODO: handle this differently
        let parent_data_proposal_hash = data_proposal
            .parent_data_proposal_hash
            .clone()
            .unwrap_or(DataProposalHash(validator_key.to_string()));

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
                .store_data_proposal(&self.crypto, &self.own_lane_id(), data_proposal)?;

        self.send_dissemination_event(DisseminationEvent::NewDpCreated {
            lane_id: self.own_lane_id(),
            data_proposal_hash: data_proposal_hash.clone(),
        })?;
        self.send_dissemination_event(DisseminationEvent::DpStored {
            lane_id: self.own_lane_id(),
            data_proposal_hash,
            cumul_size,
        })?;

        Ok(())
    }

    pub(super) fn handle_api_message(&mut self, command: RestApiMessage) -> Result<()> {
        match command {
            RestApiMessage::NewTx(tx) => self.on_new_api_tx(tx)?,
        }
        Ok(())
    }

    pub(super) fn handle_tcp_server_message(&mut self, command: TcpServerMessage) -> Result<()> {
        match command {
            TcpServerMessage::NewTx(tx) => self.on_new_api_tx(tx)?,
        }
        Ok(())
    }

    pub(super) fn on_new_api_tx(&mut self, tx: Transaction) -> Result<()> {
        // This is annoying to run in tests because we don't have the event loop setup, so go synchronous.
        #[cfg(test)]
        self.on_new_tx(tx.clone())?;
        #[cfg(not(test))]
        self.inner.processing_txs.spawn_on(
            async move {
                tx.hashed();
                Ok(tx)
            },
            self.inner.long_tasks_runtime.handle(),
        );
        Ok(())
    }

    pub(super) fn on_new_tx(&mut self, tx: Transaction) -> Result<()> {
        // TODO: Verify fees ?

        let tx_type: &'static str = (&tx.transaction_data).into();
        trace!("Tx {} received in mempool", tx_type);

        match tx.transaction_data {
            TransactionData::Blob(ref blob_tx) => {
                debug!("Got new blob tx {}", tx.hashed());
                if blob_tx.blobs.len() > 20 {
                    bail!(
                        "Blob transaction has too many blobs: {}",
                        blob_tx.blobs.len()
                    );
                }
            }
            TransactionData::Proof(ref proof_tx) => {
                debug!(
                    "Got new proof tx {} for {}",
                    tx.hashed(),
                    proof_tx.contract_name
                );
                self.inner.processing_txs.spawn_on(
                    async move {
                        let tx =
                            Self::process_proof_tx(tx).context("Processing proof tx in blocker")?;
                        Ok(tx)
                    },
                    self.inner.long_tasks_runtime.handle(),
                );

                return Ok(());
            }
            TransactionData::VerifiedProof(ref proof_tx) => {
                debug!(
                    "Got verified proof tx {} for {}",
                    tx.hashed(),
                    proof_tx.contract_name.clone()
                );
            }
        }

        let tx_type: &'static str = (&tx.transaction_data).into();

        let tx_hash = tx.hashed();
        if self.waiting_dissemination_txs.contains_key(&tx_hash) {
            debug!("Dropping duplicate tx {}", tx_hash);
            self.metrics.drop_api_tx(tx_type);
        } else {
            self.waiting_dissemination_txs
                .insert(tx_hash.clone(), tx.clone());

            self.metrics.add_api_tx(tx_type);
        }

        self.metrics
            .snapshot_pending_tx(self.waiting_dissemination_txs.len());

        Ok(())
    }

    fn process_proof_tx(mut tx: Transaction) -> Result<Transaction> {
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
                        original_proof_hash: ProofDataHash("todo?".to_owned()),
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
                assert_eq!(parent_data_proposal_hash, DataProposalHash(ctx.mempool.crypto.validator_pubkey().to_string()));
                assert_eq!(txs, actual_txs);
            }
        );
        assert_eq!(dp.txs, actual_txs);

        // Assert that pending_tx has been flushed
        assert!(ctx.mempool.waiting_dissemination_txs.is_empty());

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
            ctx.mempool.get_last_data_prop_hash_in_own_lane().unwrap(),
            dp.hashed()
        );

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_send_poda_update() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;
        let pubkey = (*ctx.mempool.crypto).clone();

        // Adding 4 other validators
        // Total voting_power = 500; f = 167 --> You need at least 2 signatures to send PoDAUpdate
        let crypto2 = BlstCrypto::new("validator2").unwrap();
        let crypto3 = BlstCrypto::new("validator3").unwrap();
        let crypto4 = BlstCrypto::new("validator4").unwrap();
        let crypto5 = BlstCrypto::new("validator5").unwrap();
        ctx.setup_node(&[pubkey, crypto2.clone(), crypto3.clone(), crypto4, crypto5]);

        let register_tx = make_register_contract_tx(ContractName::new("test1"));
        let dp = ctx.create_data_proposal(None, &[register_tx]);
        ctx.process_new_data_proposal(dp)?;
        ctx.timer_tick().await?;

        let data_proposal = match ctx.assert_broadcast("DataProposal").await.msg {
            MempoolNetMessage::DataProposal(_, _, dp) => dp,
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
            .handle_net_message(
                crypto2.sign_msg_with_header(signed_msg2)?,
                &ctx.mempool_sync_request_sender,
            )
            .await?;
        ctx.mempool
            .handle_net_message(
                crypto3.sign_msg_with_header(signed_msg3)?,
                &ctx.mempool_sync_request_sender,
            )
            .await?;

        // Assert that PoDAReady message is sent to dissemination
        match ctx
            .dissemination_event_receiver
            .recv()
            .await
            .unwrap()
            .into_message()
        {
            DisseminationEvent::PoDAReady {
                data_proposal_hash,
                signatures,
                ..
            } => {
                assert_eq!(data_proposal_hash, data_proposal.hashed());
                assert_eq!(signatures.len(), 2);
            }
            _ => panic!("Expected PoDAReady message"),
        };

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_broadcast_rehydrates_proofs() -> Result<()> {
        use crate::model::{
            BlobProofOutput, ContractName, HyliOutput, ProgramId, ProofData, ProofDataHash,
            Transaction, TransactionData, VerifiedProofTransaction, Verifier,
        };

        let mut ctx = MempoolTestCtx::new("mempool").await;

        // Bond self and another validator so broadcast happens
        let crypto2 = BlstCrypto::new("validator2").unwrap();
        let self_crypto = (*ctx.mempool.crypto).clone();
        ctx.setup_node(&[self_crypto, crypto2.clone()]);

        // Build DP with a VerifiedProof tx including the proof
        let proof = ProofData(vec![9, 9, 9, 9]);
        let proof_hash = ProofDataHash(proof.hashed().0);
        let vpt = VerifiedProofTransaction {
            contract_name: ContractName::new("rehydrate"),
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

        // Store DP locally (will strip proofs and store them side-by-side)
        ctx.process_new_data_proposal(dp.clone())?;

        // Trigger dissemination flow
        ctx.timer_tick().await?;

        // We should broadcast a DataProposal with rehydrated proofs
        let broadcast = ctx.assert_broadcast("DataProposal").await;
        let dp_broadcast = match broadcast.msg {
            MempoolNetMessage::DataProposal(_, _, dp) => dp,
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
        let lane = LaneId(ctx.validator_pubkey().clone());
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
        assert!(ctx
            .mempool
            .handle_net_message(
                temp_crypto.sign_msg_with_header(signed_msg)?,
                &ctx.mempool_sync_request_sender,
            )
            .await
            .is_err());

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
        ctx.add_trusted_validator(crypto2.validator_pubkey());

        let signed_msg = create_data_vote(&crypto2, lane_id.clone(), data_proposal_hash, size)?;

        ctx.mempool
            .handle_net_message(
                crypto2.sign_msg_with_header(signed_msg)?,
                &ctx.mempool_sync_request_sender,
            )
            .await
            .expect("should handle net message");

        // Assert that we added the vote to the signatures
        let ((LaneEntryMetadata { signatures, .. }, _), _) =
            ctx.last_lane_entry(&LaneId(ctx.validator_pubkey().clone()));

        assert_eq!(signatures.len(), 2);
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_receiving_vote_for_unknown_data_proposal() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;

        // Add new validator
        let crypto2 = BlstCrypto::new("2").unwrap();
        ctx.add_trusted_validator(crypto2.validator_pubkey());

        let signed_msg = create_data_vote(
            &crypto2,
            ctx.mempool.own_lane_id(),
            DataProposalHash("non_existent".to_owned()),
            LaneBytesSize(0),
        )?;

        assert!(ctx
            .mempool
            .handle_net_message(
                crypto2.sign_msg_with_header(signed_msg)?,
                &ctx.mempool_sync_request_sender,
            )
            .await
            .is_err());
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_redisseminate_multiple_data_proposals_four_nodes() -> Result<()> {
        // Crée 4 mempools/nœuds
        let mut ctx1 = MempoolTestCtx::new("node1").await;
        let mut ctx2 = MempoolTestCtx::new("node2").await;
        let mut ctx3 = MempoolTestCtx::new("node3").await;
        let mut ctx4 = MempoolTestCtx::new("node4").await;

        // Ajoute chaque nœud comme validateur des autres
        let all_pubkeys = vec![
            ctx1.validator_pubkey().clone(),
            ctx2.validator_pubkey().clone(),
            ctx3.validator_pubkey().clone(),
            ctx4.validator_pubkey().clone(),
        ];
        for ctx in [&mut ctx1, &mut ctx2, &mut ctx3, &mut ctx4] {
            for pk in &all_pubkeys {
                ctx.add_trusted_validator(pk);
            }
        }

        // Soumets deux transactions à ctx1 pour générer deux DPs
        let tx1 = make_register_contract_tx(ContractName::new("test1"));
        let tx2 = make_register_contract_tx(ContractName::new("test2"));
        ctx1.submit_tx(&tx1);
        ctx1.timer_tick().await?;
        // ctx1.handle_processed_data_proposals().await;
        ctx1.submit_tx(&tx2);
        ctx1.timer_tick().await?;
        // ctx1.handle_processed_data_proposals().await;

        let mut dps = vec![];
        for _ in 0..2 {
            match ctx1.assert_broadcast("DataProposal").await.msg {
                MempoolNetMessage::DataProposal(_, hash, dp) => dps.push((hash, dp)),
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
            .find(|(_, dp)| dp.parent_data_proposal_hash.is_none())
            .expect("oldest dp should exist");
        ctx1.maybe_disseminate_dp(&ctx1.own_lane(), oldest_hash)
            .expect("disseminate");

        // Récupère les deux DataProposals broadcastées par ctx1
        let mut dps = vec![];
        for _ in 0..1 {
            match ctx1.assert_broadcast("DataProposal").await.msg {
                MempoolNetMessage::DataProposal(_, hash, dp) => dps.push((hash, dp)),
                _ => panic!("Expected DataProposal message"),
            }
        }

        assert!(dps.len() == 1, "Should have the oldest DataProposal");
        assert_eq!(dps[0].1.txs.len(), 1);
        assert_eq!(dps[0].1.txs[0], tx1);

        Ok(())
    }
}

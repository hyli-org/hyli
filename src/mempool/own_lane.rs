//! Logic for processing the API inbound TXs in the mempool.

use crate::{bus::BusClientSender, model::*};

use anyhow::{bail, Context, Result};
use client_sdk::tcp_client::TcpServerMessage;
use futures::StreamExt;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, trace};

use super::verifiers::{verify_proof, verify_recursive_proof};
use super::{api::RestApiMessage, storage::Storage};
use super::{KnownContracts, MempoolNetMessage, ValidatorDAG};

impl super::Mempool {
    fn get_last_data_prop_hash_in_own_lane(&self) -> Option<DataProposalHash> {
        self.lanes.get_lane_hash_tip(&self.own_lane_id()).cloned()
    }

    pub(super) fn own_lane_id(&self) -> LaneId {
        LaneId(self.crypto.validator_pubkey().clone())
    }

    pub(super) fn on_data_vote(&mut self, vdag: ValidatorDAG) -> Result<()> {
        self.metrics.on_data_vote.add(1, &[]);

        let validator = vdag.signature.validator.clone();
        let data_proposal_hash = vdag.msg.0.clone();
        debug!(
            "Vote from {} on own lane {}, dp {}",
            validator,
            self.own_lane_id(),
            data_proposal_hash
        );
        let lane_id = self.own_lane_id();

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
            self.broadcast_net_message(MempoolNetMessage::PoDAUpdate(
                data_proposal_hash,
                signatures,
            ))?;
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
        data_proposal_hash: DataProposalHash,
    ) -> Result<bool> {
        trace!("🐣 Create new data proposal");

        self.register_new_data_proposal(data_proposal)?;

        // TODO: when we have a smarter system, we should probably not trigger this here
        // to make the event loop more efficient.
        self.disseminate_data_proposals(Some(data_proposal_hash))
            .await
    }

    /// If only_dp_with_hash is Some, only disseminate the DP with the specified hash. If None, disseminate any pending DPs.
    /// Returns true if we did disseminate something
    pub(super) async fn disseminate_data_proposals(
        &mut self,
        only_dp_with_hash: Option<DataProposalHash>,
    ) -> Result<bool> {
        trace!("🌝 Disseminate data proposals");

        let last_cut = self
            .last_ccp
            .as_ref()
            .map(|ccp| ccp.consensus_proposal.cut.clone());

        // Check for each pending DataProposal if it has enough signatures
        let cloned_lanes = self.lanes.clone();
        let own_lane_id = self.own_lane_id();
        let mut entries_stream =
            Box::pin(cloned_lanes.get_pending_entries_in_lane(&own_lane_id, last_cut));

        while let Some(stream_entry) = entries_stream.next().await {
            let (entry_metadata, dp_hash) = stream_entry?;
            // If only_dp_with_hash is Some, we only disseminate that one, skip all others.
            if let Some(ref only_dp_with_hash) = only_dp_with_hash {
                if &dp_hash != only_dp_with_hash {
                    continue;
                }
            }
            let Some(data_proposal) = self.lanes.get_dp_by_hash(&self.own_lane_id(), &dp_hash)?
            else {
                bail!(
                    "Can't find DataProposal {} in lane {}",
                    dp_hash,
                    self.own_lane_id()
                );
            };

            // If there's only 1 signature (=own signature), broadcast it to everyone
            let there_are_other_validators =
                !self.staking.is_bonded(self.crypto.validator_pubkey())
                    || self.staking.bonded().len() >= 2;
            if entry_metadata.signatures.len() == 1 && there_are_other_validators {
                debug!(
                    "🚗 Broadcast DataProposal {} ({} validators, {} txs)",
                    data_proposal.hashed(),
                    self.staking.bonded().len(),
                    data_proposal.txs.len()
                );
                self.metrics
                    .dp_disseminations
                    .add(self.staking.bonded().len() as u64, &[]);
                self.broadcast_net_message(MempoolNetMessage::DataProposal(
                    data_proposal.hashed(),
                    data_proposal.clone(),
                ))?;

                // TODO: for performance reasons in the event loop, we'll only process the first item for now.
                return Ok(true);
            } else {
                // If None, rebroadcast it to every validator that has not yet signed it
                let validator_that_has_signed: HashSet<&ValidatorPublicKey> = entry_metadata
                    .signatures
                    .iter()
                    .map(|s| &s.signature.validator)
                    .collect();

                // No PoA means we rebroadcast the DataProposal for non present voters
                let only_for: HashSet<ValidatorPublicKey> = self
                    .staking
                    .bonded()
                    .iter()
                    .filter(|pubkey| !validator_that_has_signed.contains(pubkey))
                    .cloned()
                    .collect();

                if only_for.is_empty() {
                    continue;
                }

                let Some(data_proposal) =
                    self.lanes.get_dp_by_hash(&self.own_lane_id(), &dp_hash)?
                else {
                    bail!(
                        "Can't find DataProposal {} in lane {}",
                        dp_hash,
                        self.own_lane_id()
                    );
                };

                debug!(
                    "🚗 Rebroadcast DataProposal {} (only for {} validators, {} txs)",
                    &data_proposal.hashed(),
                    only_for.len(),
                    &data_proposal.txs.len()
                );

                self.metrics
                    .dp_disseminations
                    .add(only_for.len() as u64, &[]);

                self.broadcast_only_for_net_message(
                    only_for,
                    MempoolNetMessage::DataProposal(data_proposal.hashed(), data_proposal.clone()),
                )?;
                // TODO: for performance reasons in the event loop, we'll only process the first item for now.
                return Ok(true);
            }
        }

        Ok(false)
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
        while cumulative_size < 40_000 && current_idx < self.waiting_dissemination_txs.len() {
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

        self.bus
            .send(MempoolStatusEvent::DataProposalCreated {
                data_proposal_hash: data_proposal.hashed(),
                txs_metadatas,
            })
            .context("Sending MempoolStatusEvent DataProposalCreated")?;

        self.metrics.created_data_proposals.add(1, &[]);

        self.lanes
            .store_data_proposal(&self.crypto, &self.own_lane_id(), data_proposal)?;

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
                // TODO: we should check if the registration handler contract exists.
                // TODO: would be good to not need to clone here.
                self.handle_hyle_contract_registration(blob_tx);
            }
            TransactionData::Proof(ref proof_tx) => {
                debug!(
                    "Got new proof tx {} for {}",
                    tx.hashed(),
                    proof_tx.contract_name
                );
                let kc = self.known_contracts.clone();
                self.inner.processing_txs.spawn_on(
                    async move {
                        let tx = Self::process_proof_tx(kc, tx)
                            .context("Processing proof tx in blocker")?;
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

            // dbg!(&self.waiting);

            self.metrics.add_api_tx(tx_type);
            let status_event = MempoolStatusEvent::WaitingDissemination {
                // TODO: handle this differently somehow. For now, this works as we always drain waiting tx right up.
                parent_data_proposal_hash: self
                    .get_last_data_prop_hash_in_own_lane()
                    .unwrap_or(DataProposalHash(self.crypto.validator_pubkey().to_string())),
                tx,
            };

            self.bus
                .send(status_event)
                .context("Sending Status event for TX")?;
        }

        self.metrics
            .snapshot_pending_tx(self.waiting_dissemination_txs.len());

        Ok(())
    }

    fn process_proof_tx(
        known_contracts: Arc<std::sync::RwLock<KnownContracts>>,
        mut tx: Transaction,
    ) -> Result<Transaction> {
        let TransactionData::Proof(proof_transaction) = tx.transaction_data else {
            bail!("Can only process ProofTx");
        };
        // Verify and extract proof
        #[allow(clippy::expect_used, reason = "not held across await")]
        let (verifier, program_id) = known_contracts
            .read()
            .expect("logic error")
            .0
            .get(&proof_transaction.contract_name)
            .context("Contract unknown")?
            .clone();

        let is_recursive = proof_transaction.contract_name.0 == "risc0-recursion";

        let (hyle_outputs, program_ids) = if is_recursive {
            let (program_ids, hyle_outputs) =
                verify_recursive_proof(&proof_transaction.proof, &verifier, &program_id)
                    .context("verify_rec_proof")?;
            (hyle_outputs, program_ids)
        } else {
            let hyle_outputs = verify_proof(&proof_transaction.proof, &verifier, &program_id)
                .context("verify_proof")?;
            let len = hyle_outputs.len();
            (hyle_outputs, vec![program_id.clone(); len])
        };

        let tx_hashes = hyle_outputs
            .iter()
            .map(|ho| ho.tx_hash.clone())
            .collect::<Vec<_>>();

        std::iter::zip(&tx_hashes, std::iter::zip(&hyle_outputs, &program_ids)).for_each(
            |(blob_tx_hash, (hyle_output, program_id))| {
                debug!(
                    "Blob tx hash {} verified with hyle output {:?} and program id {}",
                    blob_tx_hash,
                    hyle_output,
                    hex::encode(&program_id.0)
                );
            },
        );

        tx.transaction_data = TransactionData::VerifiedProof(VerifiedProofTransaction {
            proof_hash: proof_transaction.proof.hashed(),
            proof_size: proof_transaction.estimate_size(),
            proof: Some(proof_transaction.proof),
            contract_name: proof_transaction.contract_name.clone(),
            is_recursive,
            proven_blobs: std::iter::zip(tx_hashes, std::iter::zip(hyle_outputs, program_ids))
                .map(
                    |(blob_tx_hash, (hyle_output, program_id))| BlobProofOutput {
                        original_proof_hash: ProofDataHash("todo?".to_owned()),
                        blob_tx_hash: blob_tx_hash.clone(),
                        hyle_output,
                        program_id,
                    },
                )
                .collect(),
        });

        Ok(tx)
    }
}

#[cfg(test)]
pub mod test {
    use core::panic;

    use super::*;
    use crate::{
        mempool::storage::LaneEntryMetadata, p2p::network::HeaderSigner,
        tests::autobahn_testing::assert_chanmsg_matches,
    };
    use anyhow::Result;
    use hyle_crypto::BlstCrypto;

    use crate::mempool::test::*;

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

        assert_chanmsg_matches!(
            ctx.mempool_status_event_receiver,
            MempoolStatusEvent::WaitingDissemination { parent_data_proposal_hash, tx } => {
                assert_eq!(parent_data_proposal_hash, DataProposalHash(ctx.mempool.crypto.validator_pubkey().to_string()));
                assert_eq!(tx,register_tx);
            }
        );
        assert_chanmsg_matches!(
            ctx.mempool_status_event_receiver,
            MempoolStatusEvent::WaitingDissemination { parent_data_proposal_hash, tx } => {
                assert_eq!(parent_data_proposal_hash, DataProposalHash(ctx.mempool.crypto.validator_pubkey().to_string()));
                assert_eq!(tx,register_tx_2);
            }
        );
        assert_chanmsg_matches!(
            ctx.mempool_status_event_receiver,
            MempoolStatusEvent::WaitingDissemination { parent_data_proposal_hash, tx } => {
                assert_eq!(parent_data_proposal_hash, DataProposalHash(ctx.mempool.crypto.validator_pubkey().to_string()));
                assert_eq!(tx,register_tx_3);
            }
        );

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
            .get_dp_by_hash(&ctx.own_lane(), dp_hash)
            .unwrap()
            .unwrap();

        assert_eq!(
            dp.txs,
            vec![
                register_tx.clone(),
                register_tx_2.clone(),
                register_tx_3.clone(),
            ]
        );

        // Assert that pending_tx has been flushed
        assert!(ctx.mempool.waiting_dissemination_txs.is_empty());

        assert_chanmsg_matches!(
            ctx.mempool_status_event_receiver,
            MempoolStatusEvent::DataProposalCreated { data_proposal_hash, txs_metadatas } => {
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
            MempoolNetMessage::DataProposal(_, dp) => dp,
            _ => panic!("Expected DataProposal message"),
        };
        let size = LaneBytesSize(data_proposal.estimate_size() as u64);

        // Simulate receiving votes from other validators
        let signed_msg2 = create_data_vote(&crypto2, data_proposal.hashed(), size)?;
        let signed_msg3 = create_data_vote(&crypto3, data_proposal.hashed(), size)?;
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

        // Assert that PoDAUpdate message is broadcasted
        match ctx.assert_broadcast("PoDAUpdate").await.msg {
            MempoolNetMessage::PoDAUpdate(hash, signatures) => {
                assert_eq!(hash, data_proposal.hashed());
                assert_eq!(signatures.len(), 2);
            }
            _ => panic!("Expected PoDAUpdate message"),
        };

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

        let temp_crypto = BlstCrypto::new("temp_crypto").unwrap();
        let signed_msg = create_data_vote(&temp_crypto, data_proposal.hashed(), size)?;
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
        let data_proposal = ctx.create_data_proposal(None, &[register_tx.clone()]);
        ctx.process_new_data_proposal(data_proposal.clone())?;

        // Then make another validator vote on it.
        let size = LaneBytesSize(data_proposal.estimate_size() as u64);
        let data_proposal_hash = data_proposal.hashed();

        // Add new validator
        let crypto2 = BlstCrypto::new("2").unwrap();
        ctx.add_trusted_validator(crypto2.validator_pubkey());

        let signed_msg = create_data_vote(&crypto2, data_proposal_hash, size)?;

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
}

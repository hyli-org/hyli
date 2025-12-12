mod async_data_proposals;
mod native_verifier_test;

use core::panic;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::*;
use crate::bus::metrics::BusMetrics;
use crate::bus::SharedMessageBus;
use crate::model;
use crate::p2p::network::NetMessage;
use crate::{
    bus::dont_use_this::get_receiver,
    p2p::network::{HeaderSigner, MsgWithHeader},
};
use anyhow::Result;
use assertables::assert_ok;
use hyli_contract_sdk::StateCommitment;
use hyli_crypto::BlstCrypto;
use hyli_modules::bus::BusReceiver;
use hyli_modules::modules::Module;
use utils::TimestampMs;

pub struct MempoolTestCtx {
    pub name: String,
    pub out_receiver: BusReceiver<OutboundMessage>,
    pub mempool_sync_request_sender: tokio::sync::mpsc::Sender<SyncRequest>,
    pub mempool_event_receiver: BusReceiver<MempoolBlockEvent>,
    pub mempool_status_event_receiver: BusReceiver<MempoolStatusEvent>,
    pub mempool: Mempool,
}

impl MempoolTestCtx {
    pub async fn build_mempool(shared_bus: &SharedMessageBus, crypto: BlstCrypto) -> Mempool {
        let tmp_dir = tempfile::tempdir().unwrap().keep();
        let lanes = LanesStorage::new(&tmp_dir, BTreeMap::default()).unwrap();
        let bus = MempoolBusClient::new_from_bus(shared_bus.new_handle()).await;

        // Initialize Mempool
        Mempool {
            bus,
            file: None,
            conf: SharedConf::default(),
            crypto: Arc::new(crypto),
            metrics: MempoolMetrics::global("id".to_string()),
            lanes,
            inner: MempoolStore::default(),
        }
    }

    pub async fn new(name: &str) -> Self {
        let crypto = BlstCrypto::new(name).unwrap();
        let shared_bus = SharedMessageBus::new(BusMetrics::global("global".to_string()));

        let out_receiver = get_receiver::<OutboundMessage>(&shared_bus).await;
        let mempool_event_receiver = get_receiver::<MempoolBlockEvent>(&shared_bus).await;
        let mempool_status_event_receiver = get_receiver::<MempoolStatusEvent>(&shared_bus).await;
        let mempool = Self::build_mempool(&shared_bus, crypto).await;
        let mempool_sync_request_sender = mempool.start_mempool_sync();

        MempoolTestCtx {
            name: name.to_string(),
            out_receiver,
            mempool_sync_request_sender,
            mempool_event_receiver,
            mempool_status_event_receiver,
            mempool,
        }
    }

    pub fn setup_node(&mut self, cryptos: &[BlstCrypto]) {
        for other_crypto in cryptos.iter() {
            self.add_trusted_validator(other_crypto.validator_pubkey());
        }
    }

    pub fn own_lane(&self) -> LaneId {
        self.mempool
            .get_lane(self.mempool.crypto.validator_pubkey())
    }

    pub fn validator_pubkey(&self) -> &ValidatorPublicKey {
        self.mempool.crypto.validator_pubkey()
    }

    pub fn add_trusted_validator(&mut self, pubkey: &ValidatorPublicKey) {
        self.mempool
            .staking
            .stake(hex::encode(pubkey.0.clone()).into(), 100)
            .unwrap();

        self.mempool
            .staking
            .delegate_to(hex::encode(pubkey.0.clone()).into(), pubkey.clone())
            .unwrap();

        self.mempool
            .staking
            .bond(pubkey.clone())
            .expect("cannot bond trusted validator");
    }

    pub fn sign_data<T: borsh::BorshSerialize>(&self, data: T) -> Result<SignedByValidator<T>> {
        self.mempool.crypto.sign(data)
    }

    pub fn create_net_message(
        &self,
        msg: MempoolNetMessage,
    ) -> Result<MsgWithHeader<MempoolNetMessage>> {
        self.mempool.crypto.sign_msg_with_header(msg)
    }

    pub fn gen_cut(&mut self, staking: &Staking) -> Cut {
        self.mempool
            .handle_querynewcut(&mut QueryNewCut {
                staking: staking.clone(),
                full: true,
            })
            .unwrap()
    }

    pub async fn timer_tick(&mut self) -> Result<bool> {
        let Ok(true) = self.mempool.prepare_new_data_proposal() else {
            debug!("No new data proposal to prepare");
            return self.mempool.disseminate_data_proposals(None).await;
        };

        let (dp_hash, dp) = self
            .mempool
            .own_data_proposal_in_preparation
            .join_next()
            .await
            .context("join next data proposal in preparation")??;

        Ok(self.mempool.resume_new_data_proposal(dp, dp_hash).await?
            || self.mempool.disseminate_data_proposals(None).await?)
    }

    pub async fn disseminate_timer_tick(&mut self) -> Result<bool> {
        self.mempool.redisseminate_oldest_data_proposal().await
    }

    pub async fn handle_poda_update(
        &mut self,
        net_message: MsgWithHeader<MempoolNetMessage>,
        sync_request_sender: &tokio::sync::mpsc::Sender<SyncRequest>,
    ) {
        self.mempool
            .handle_net_message(net_message, sync_request_sender)
            .await
            .expect("fail to handle net message");
    }

    pub async fn handle_processed_data_proposals(&mut self) {
        let event = self
            .mempool
            .inner
            .processing_dps
            .join_next()
            .await
            .expect("No event received")
            .expect("No event received")
            .expect("No event received");
        self.mempool
            .handle_internal_event(event)
            .expect("fail to handle event");
    }

    #[track_caller]
    pub fn assert_broadcast_only_for(
        &mut self,
        description: &str,
    ) -> MsgWithHeader<MempoolNetMessage> {
        #[allow(clippy::expect_fun_call)]
        let rec = self
            .out_receiver
            .try_recv()
            .expect(format!("{description}: No message broadcasted").as_str())
            .into_message();

        match rec {
            OutboundMessage::BroadcastMessageOnlyFor(_, net_msg) => {
                if let NetMessage::MempoolMessage(msg) = net_msg {
                    msg
                } else {
                    println!(
                        "{description}: Mempool OutboundMessage message is missing, found {net_msg}"
                    );
                    self.assert_broadcast_only_for(description)
                }
            }
            _ => {
                println!(
                    "{description}: Broadcast OutboundMessage message is missing, found {rec:?}",
                );
                self.assert_broadcast_only_for(description)
            }
        }
    }
    pub fn assert_send(
        &mut self,
        to: &ValidatorPublicKey,
        description: &str,
    ) -> Pin<Box<dyn Future<Output = MsgWithHeader<MempoolNetMessage>> + '_>> {
        let to = to.clone();
        let description = description.to_string().clone();
        Box::pin(async move {
            #[allow(clippy::expect_fun_call)]
            let rec = tokio::time::timeout(Duration::from_millis(1000), self.out_receiver.recv())
                .await
                .expect(format!("{description}: No message broadcasted").as_str())
                .expect(format!("{description}: No message broadcasted").as_str())
                .into_message();

            match rec {
                OutboundMessage::SendMessage { validator_id, msg } => {
                    if let NetMessage::MempoolMessage(msg) = msg {
                        if validator_id != to {
                            panic!(
                            "{description}: Send message was sent to {validator_id} instead of {}",
                            to
                        );
                        }

                        msg
                    } else {
                        tracing::warn!("{description}: skipping {:?}", msg);
                        self.assert_send(&to, description.as_str()).await
                    }
                }
                OutboundMessage::BroadcastMessage(NetMessage::ConsensusMessage(e)) => {
                    tracing::warn!("{description}: skipping broadcast message {:?}", e);
                    self.assert_send(&to, description.as_str()).await
                }
                OutboundMessage::BroadcastMessage(els) => {
                    panic!(
                        "{description}: received broadcast message instead of send {:?}",
                        els
                    );
                }
                OutboundMessage::BroadcastMessageOnlyFor(_, NetMessage::ConsensusMessage(e)) => {
                    tracing::warn!("{description}: skipping broadcast message {:?}", e);
                    self.assert_send(&to, description.as_str()).await
                }
                OutboundMessage::BroadcastMessageOnlyFor(_, els) => {
                    panic!(
                        "{description}: received broadcast only for message instead of send {:?}",
                        els
                    );
                }
            }
        })
    }

    pub fn assert_broadcast(
        &mut self,
        description: &str,
    ) -> Pin<Box<dyn Future<Output = MsgWithHeader<MempoolNetMessage>> + '_>> {
        let description = description.to_string().clone();
        Box::pin(async move {
            #[allow(clippy::expect_fun_call)]
            let rec = tokio::time::timeout(Duration::from_millis(1000), self.out_receiver.recv())
                .await
                .expect(format!("{description}: No message broadcasted").as_str())
                .expect(format!("{description}: No message broadcasted").as_str())
                .into_message();

            match rec {
                OutboundMessage::BroadcastMessage(net_msg) => {
                    if let NetMessage::MempoolMessage(msg) = net_msg {
                        msg
                    } else {
                        println!("{description}: Mempool OutboundMessage message is missing, found {net_msg}");
                        self.assert_broadcast(description.as_str()).await
                    }
                }
                _ => {
                    println!("{description}: Broadcast OutboundMessage message is missing, found {rec:?}");
                    self.assert_broadcast(description.as_str()).await
                }
            }
        })
    }

    pub async fn handle_msg(&mut self, msg: &MsgWithHeader<MempoolNetMessage>, _err: &str) {
        debug!("📥 {} Handling message: {:?}", self.name, msg);
        self.mempool
            .handle_net_message(msg.clone(), &self.mempool_sync_request_sender)
            .await
            .expect("should handle net msg");
    }

    pub fn current_hash(&self, lane_id: &LaneId) -> Option<DataProposalHash> {
        self.mempool
            .lanes
            .lanes_tip
            .get(lane_id)
            .cloned()
            .map(|(h, _)| h)
    }

    pub fn current_size_of(&self, lane_id: &LaneId) -> Option<LaneBytesSize> {
        self.mempool
            .lanes
            .lanes_tip
            .get(lane_id)
            .cloned()
            .map(|(_, s)| s)
    }

    pub fn last_lane_entry(
        &self,
        lane_id: &LaneId,
    ) -> ((LaneEntryMetadata, DataProposal), DataProposalHash) {
        let last_dp_hash = self.current_hash(lane_id).unwrap();
        let last_metadata = self
            .mempool
            .lanes
            .get_metadata_by_hash(lane_id, &last_dp_hash)
            .unwrap()
            .unwrap();
        let last_dp = self
            .mempool
            .lanes
            .get_dp_by_hash(lane_id, &last_dp_hash)
            .unwrap()
            .unwrap();

        ((last_metadata, last_dp), last_dp_hash.clone())
    }

    pub fn current_size(&self) -> Option<LaneBytesSize> {
        let lane_id = LaneId(self.validator_pubkey().clone());
        self.current_size_of(&lane_id)
    }

    pub fn push_data_proposal(&mut self, dp: DataProposal) {
        let lane_id = LaneId(self.validator_pubkey().clone());

        let lane_size = self.current_size().unwrap();
        let size = lane_size + dp.estimate_size();
        self.mempool
            .lanes
            .put_no_verification(
                lane_id,
                (
                    LaneEntryMetadata {
                        parent_data_proposal_hash: dp.parent_data_proposal_hash.clone(),
                        cumul_size: size,
                        signatures: vec![],
                    },
                    dp,
                ),
            )
            .unwrap();
    }

    pub async fn handle_consensus_event(&mut self, consensus_proposal: ConsensusProposal) {
        self.mempool
            .handle_consensus_event(ConsensusEvent::CommitConsensusProposal(
                CommittedConsensusProposal {
                    staking: self.mempool.staking.clone(),
                    consensus_proposal,
                    certificate: AggregateSignature::default(),
                },
            ))
            .await
            .expect("Error while handling consensus event");
    }

    pub fn submit_tx(&mut self, tx: &Transaction) {
        self.mempool
            .handle_api_message(RestApiMessage::NewTx(tx.clone()))
            .unwrap();
    }

    pub fn create_data_proposal(
        &self,
        parent_hash: Option<DataProposalHash>,
        txs: &[Transaction],
    ) -> DataProposal {
        DataProposal::new(parent_hash, txs.to_vec())
    }

    pub fn create_data_proposal_on_top(
        &mut self,
        lane_id: LaneId,
        txs: &[Transaction],
    ) -> DataProposal {
        DataProposal::new(self.current_hash(&lane_id), txs.to_vec())
    }

    pub fn process_new_data_proposal(&mut self, dp: DataProposal) -> Result<()> {
        self.mempool.lanes.store_data_proposal(
            &self.mempool.crypto,
            &LaneId(self.mempool.crypto.validator_pubkey().clone()),
            dp,
        )?;
        Ok(())
    }

    pub async fn process_cut_with_dp(
        &mut self,
        leader: &ValidatorPublicKey,
        dp_hash: &DataProposalHash,
        cumul_size: LaneBytesSize,
        slot: u64,
    ) -> Result<Cut> {
        let cut = vec![(
            LaneId(leader.clone()),
            dp_hash.clone(),
            cumul_size,
            AggregateSignature::default(),
        )];

        self.mempool
            .handle_consensus_event(ConsensusEvent::CommitConsensusProposal(
                CommittedConsensusProposal {
                    staking: self.mempool.staking.clone(),
                    consensus_proposal: model::ConsensusProposal {
                        slot,
                        cut: cut.clone(),
                        staking_actions: vec![],
                        timestamp: TimestampMs(777),
                        parent_hash: ConsensusProposalHash("test".to_string()),
                    },
                    certificate: AggregateSignature::default(),
                },
            ))
            .await?;

        Ok(cut)
    }
}

pub fn create_data_vote(
    crypto: &BlstCrypto,
    hash: DataProposalHash,
    size: LaneBytesSize,
) -> Result<MempoolNetMessage> {
    Ok(MempoolNetMessage::DataVote(crypto.sign((hash, size))?))
}

pub fn make_register_contract_tx(name: ContractName) -> Transaction {
    BlobTransaction::new(
        "hyli@hyli",
        vec![RegisterContractAction {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![0, 1, 2, 3]),
            contract_name: name,
            ..Default::default()
        }
        .as_blob("hyli".into())],
    )
    .into()
}

#[test_log::test(tokio::test)]
async fn test_sending_sync_request() -> Result<()> {
    let mut ctx = MempoolTestCtx::new("mempool").await;
    let crypto2 = BlstCrypto::new("2").unwrap();
    let pubkey2 = crypto2.validator_pubkey();

    ctx.handle_consensus_event(ConsensusProposal {
        cut: vec![(
            LaneId(pubkey2.clone()),
            DataProposalHash("dp_hash_in_cut".to_owned()),
            LaneBytesSize::default(),
            PoDA::default(),
        )],
        ..ConsensusProposal::default()
    })
    .await;

    // Assert that we send a SyncReply
    match ctx
        .assert_send(crypto2.validator_pubkey(), "SyncReply")
        .await
        .msg
    {
        MempoolNetMessage::SyncRequest(from, to) => {
            assert_eq!(from, None);
            assert_eq!(to, Some(DataProposalHash("dp_hash_in_cut".to_owned())));
        }
        _ => panic!("Expected SyncReply message"),
    };
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_receiving_sync_request() -> Result<()> {
    let mut ctx = MempoolTestCtx::new("mempool").await;

    // Store the DP locally.
    let register_tx = make_register_contract_tx(ContractName::new("test1"));
    let data_proposal = ctx.create_data_proposal(None, std::slice::from_ref(&register_tx));
    ctx.process_new_data_proposal(data_proposal.clone())?;

    // Since mempool is alone, no broadcast
    let (..) = ctx.last_lane_entry(&LaneId(ctx.validator_pubkey().clone()));

    // Add new validator
    let crypto2 = BlstCrypto::new("2").unwrap();
    ctx.add_trusted_validator(crypto2.validator_pubkey());

    for _ in 1..5 {
        let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncRequest(
            None,
            Some(data_proposal.hashed()),
        ))?;

        ctx.mempool
            .handle_net_message(signed_msg, &ctx.mempool_sync_request_sender)
            .await
            .expect("should handle net message");
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    for _ in 1..5 {
        let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncRequest(
            None,
            Some(data_proposal.hashed()),
        ))?;

        ctx.mempool
            .handle_net_message(signed_msg, &ctx.mempool_sync_request_sender)
            .await
            .expect("should handle net message");
    }

    // Assert that we send a SyncReply
    match ctx
        .assert_send(crypto2.validator_pubkey(), "SyncReply")
        .await
        .msg
    {
        MempoolNetMessage::SyncReply(_metadata, data_proposal_r) => {
            assert_eq!(data_proposal_r, data_proposal);
        }
        _ => panic!("Expected SyncReply message"),
    };

    assert!(ctx.out_receiver.is_empty());
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_receiving_sync_requests_multiple_dps() -> Result<()> {
    let mut ctx = MempoolTestCtx::new("mempool").await;

    // Store the DP locally.
    let register_tx = make_register_contract_tx(ContractName::new("test1"));
    let data_proposal = ctx.create_data_proposal(None, std::slice::from_ref(&register_tx));
    ctx.process_new_data_proposal(data_proposal.clone())?;

    let data_proposal2 = ctx.create_data_proposal(
        Some(data_proposal.hashed()),
        std::slice::from_ref(&register_tx),
    );
    ctx.process_new_data_proposal(data_proposal2.clone())?;

    // Since mempool is alone, no broadcast
    let (..) = ctx.last_lane_entry(&LaneId(ctx.validator_pubkey().clone()));

    // Add new validator
    let crypto2 = BlstCrypto::new("2").unwrap();
    ctx.add_trusted_validator(crypto2.validator_pubkey());

    // Sync request for the interval up to data proposal
    for _ in 1..3 {
        let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncRequest(
            None,
            Some(data_proposal.hashed()),
        ))?;

        ctx.mempool
            .handle_net_message(signed_msg, &ctx.mempool_sync_request_sender)
            .await
            .expect("should handle net message");
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Sync request for the whole interval
    for _ in 1..3 {
        let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncRequest(
            None,
            Some(data_proposal2.hashed()),
        ))?;

        ctx.mempool
            .handle_net_message(signed_msg, &ctx.mempool_sync_request_sender)
            .await
            .expect("should handle net message");
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Sync request for the whole interval
    for _ in 1..3 {
        let signed_msg =
            crypto2.sign_msg_with_header(MempoolNetMessage::SyncRequest(None, None))?;

        ctx.mempool
            .handle_net_message(signed_msg, &ctx.mempool_sync_request_sender)
            .await
            .expect("should handle net message");
    }

    // Assert that we send a SyncReply #1
    match ctx
        .assert_send(crypto2.validator_pubkey(), "SyncReply")
        .await
        .msg
    {
        MempoolNetMessage::SyncReply(_metadata, data_proposal_r) => {
            assert_eq!(data_proposal_r, data_proposal);
        }
        _ => panic!("Expected SyncReply message"),
    };
    // Assert that we send a SyncReply #2
    match ctx
        .assert_send(crypto2.validator_pubkey(), "SyncReply")
        .await
        .msg
    {
        MempoolNetMessage::SyncReply(_metadata, data_proposal_r) => {
            assert_eq!(data_proposal_r, data_proposal2);
        }
        _ => panic!("Expected SyncReply message"),
    };

    assert!(ctx.out_receiver.is_empty());
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_receiving_sync_reply() -> Result<()> {
    let mut ctx = MempoolTestCtx::new("mempool").await;

    // Create a DP and simulate we requested it.
    let register_tx = make_register_contract_tx(ContractName::new("test1"));
    let data_proposal = ctx.create_data_proposal(None, std::slice::from_ref(&register_tx));
    let cumul_size = LaneBytesSize(data_proposal.estimate_size() as u64);

    // Add new validator
    let crypto2 = BlstCrypto::new("2").unwrap();
    let crypto3 = BlstCrypto::new("3").unwrap();

    ctx.add_trusted_validator(crypto2.validator_pubkey());

    // First: the message is from crypto2, but the DP is not signed correctly
    let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncReply(
        LaneEntryMetadata {
            parent_data_proposal_hash: None,
            cumul_size,
            signatures: vec![crypto3
                .sign((data_proposal.hashed(), cumul_size))
                .expect("should sign")],
        },
        data_proposal.clone(),
    ))?;

    let handle = ctx
        .mempool
        .handle_net_message(signed_msg.clone(), &ctx.mempool_sync_request_sender)
        .await;
    assert_eq!(
        handle.expect_err("should fail").to_string(),
        format!(
            "At least one lane entry is missing signature from {}",
            crypto2.validator_pubkey()
        )
    );

    // Second: the message is NOT from crypto2, but the DP is signed by crypto2
    let signed_msg = crypto3.sign_msg_with_header(MempoolNetMessage::SyncReply(
        LaneEntryMetadata {
            parent_data_proposal_hash: None,
            cumul_size,
            signatures: vec![crypto2
                .sign((data_proposal.hashed(), cumul_size))
                .expect("should sign")],
        },
        data_proposal.clone(),
    ))?;

    // This actually fails - we don't know how to handle it
    let handle = ctx
        .mempool
        .handle_net_message(signed_msg.clone(), &ctx.mempool_sync_request_sender)
        .await;
    assert_eq!(
        handle.expect_err("should fail").to_string(),
        format!(
            "At least one lane entry is missing signature from {}",
            crypto3.validator_pubkey()
        )
    );

    // Third: the message is from crypto2, the signature is from crypto2, but the message is wrong
    let signed_msg = crypto3.sign_msg_with_header(MempoolNetMessage::SyncReply(
        LaneEntryMetadata {
            parent_data_proposal_hash: None,
            cumul_size,
            signatures: vec![crypto2
                .sign((DataProposalHash("non_existent".to_owned()), cumul_size))
                .expect("should sign")],
        },
        data_proposal.clone(),
    ))?;

    // This actually fails - we don't know how to handle it
    let handle = ctx
        .mempool
        .handle_net_message(signed_msg.clone(), &ctx.mempool_sync_request_sender)
        .await;
    assert_eq!(
        handle.expect_err("should fail").to_string(),
        format!(
            "At least one lane entry is missing signature from {}",
            crypto3.validator_pubkey()
        )
    );

    // Fourth: the message is from crypto2, the signature is from crypto2, but the size is wrong
    let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncReply(
        LaneEntryMetadata {
            parent_data_proposal_hash: None,
            cumul_size: LaneBytesSize(0),
            signatures: vec![crypto2
                .sign((data_proposal.hashed(), cumul_size))
                .expect("should sign")],
        },
        data_proposal.clone(),
    ))?;

    // This actually fails - we don't know how to handle it
    let handle = ctx
        .mempool
        .handle_net_message(signed_msg.clone(), &ctx.mempool_sync_request_sender)
        .await;
    assert_eq!(
        handle.expect_err("should fail").to_string(),
        format!(
            "At least one lane entry is missing signature from {}",
            crypto2.validator_pubkey()
        )
    );

    // Final case: message is correct
    let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncReply(
        LaneEntryMetadata {
            parent_data_proposal_hash: None,
            cumul_size,
            signatures: vec![crypto2
                .sign((data_proposal.hashed(), cumul_size))
                .expect("should sign")],
        },
        data_proposal.clone(),
    ))?;

    let handle = ctx
        .mempool
        .handle_net_message(signed_msg.clone(), &ctx.mempool_sync_request_sender)
        .await;
    assert_ok!(handle, "Should handle net message");

    // Assert that the lane entry was added
    assert!(ctx.mempool.lanes.contains(
        &ctx.mempool.get_lane(crypto2.validator_pubkey()),
        &data_proposal.hashed()
    ));

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_sync_request_single_dp() -> Result<()> {
    let mut ctx = MempoolTestCtx::new("mempool").await;
    let lane_id = ctx.mempool.own_lane_id().clone();
    let crypto = ctx.mempool.crypto.clone();

    // Create a chain of 3 DataProposals
    let dp1 = DataProposal::new(None, vec![]);
    let _dp1_size = LaneBytesSize(dp1.estimate_size() as u64);
    let dp1_hash = dp1.hashed();
    ctx.mempool
        .lanes
        .store_data_proposal(&crypto, &lane_id, dp1.clone())?;

    let dp2 = DataProposal::new(Some(dp1_hash.clone()), vec![]);
    let dp2_size = LaneBytesSize(dp2.estimate_size() as u64);
    let dp2_hash = dp2.hashed();
    ctx.mempool
        .lanes
        .store_data_proposal(&crypto, &lane_id, dp2.clone())?;

    let dp3 = DataProposal::new(Some(dp2_hash.clone()), vec![]);
    let _dp3_size = LaneBytesSize(dp3.estimate_size() as u64);
    let _dp3_hash = dp3.hashed();
    ctx.mempool
        .lanes
        .store_data_proposal(&crypto, &lane_id, dp3.clone())?;

    // Send a sync request for dp2 only
    ctx.mempool
        .on_sync_request(
            &ctx.mempool_sync_request_sender,
            None,
            Some(dp2_hash.clone()),
            crypto.validator_pubkey().clone(),
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify that only dp2 was sent in the sync reply
    match ctx
        .assert_send(&crypto.validator_pubkey().clone(), "SyncReply")
        .await
        .msg
    {
        MempoolNetMessage::SyncReply(metadata, dp) => {
            assert_eq!(dp, dp2);
            assert_eq!(metadata.parent_data_proposal_hash, Some(dp1_hash));
            assert_eq!(metadata.cumul_size, dp2_size);
            assert_eq!(metadata.signatures.len(), 1);
        }
        _ => panic!("Expected SyncReply message"),
    }

    // Verify no other messages were sent
    assert!(ctx.out_receiver.try_recv().is_err());

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_serialization_deserialization() -> Result<()> {
    let mut ctx = MempoolTestCtx::new("mempool").await;
    ctx.mempool.file = Some(".".into());

    assert!(Mempool::save_on_disk(
        ctx.mempool
            .file
            .clone()
            .unwrap()
            .join("test-mempool.bin")
            .as_path(),
        &ctx.mempool.inner
    )
    .is_ok());

    assert!(Mempool::load_from_disk::<MempoolStore>(
        ctx.mempool.file.unwrap().join("test-mempool.bin").as_path(),
    )
    .is_some());

    std::fs::remove_file("./test-mempool.bin").expect("Failed to delete test-mempool.bin");

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_get_latest_car_and_new_cut() {
    let mut ctx = MempoolTestCtx::new("mempool").await;

    let dp_orig = ctx.create_data_proposal(None, &[]);
    ctx.process_new_data_proposal(dp_orig.clone()).unwrap();

    let staking = Staking::new();

    let latest = ctx
        .mempool
        .lanes
        .get_latest_car(&ctx.own_lane(), &staking, None)
        .unwrap();
    assert!(latest.is_none());

    // Force some signature for f+1 check if needed:
    // This requires more advanced stubbing of Staking if you want a real test.

    let cut = ctx
        .mempool
        .handle_querynewcut(&mut QueryNewCut {
            staking: staking.clone(),
            full: true,
        })
        .unwrap();
    assert_eq!(0, cut.len());
}

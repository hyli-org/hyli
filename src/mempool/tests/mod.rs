mod async_data_proposals;
mod multi_lane_dispatch;
mod native_verifier_test;
mod sync_reply;

use core::panic;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::bus::metrics::BusMetrics;
use crate::bus::SharedMessageBus;
use crate::mempool::dissemination::DisseminationManager;
use crate::mempool::storage::LaneEntryMetadata;
use crate::model;
use crate::p2p::network::NetMessage;
use crate::utils::conf::Conf;
use crate::{
    bus::dont_use_this::get_receiver,
    p2p::network::{HeaderSigner, MsgWithHeader},
};
use anyhow::Result;
use assertables::assert_ok;
use hyli_contract_sdk::StateCommitment;
use hyli_crypto::BlstCrypto;
use hyli_modules::bus::BusReceiver;
use hyli_modules::modules::BuildApiContextInner;
use hyli_modules::modules::Module;
use std::path::{Path, PathBuf};
use tokio::sync::broadcast::error::TryRecvError;
use utils::TimestampMs;

pub struct MempoolTestCtx {
    pub name: String,
    pub out_receiver: BusReceiver<OutboundMessage>,
    pub mempool_event_receiver: BusReceiver<MempoolBlockEvent>,
    pub mempool_status_event_receiver: BusReceiver<MempoolStatusEvent>,
    pub dissemination_event_receiver: BusReceiver<DisseminationEvent>,
    pub mempool: Mempool,
    pub dissemination_manager: DisseminationManager,
    pub data_dir: PathBuf,
}

impl MempoolTestCtx {
    async fn build_mempool(
        shared_bus: &SharedMessageBus,
        crypto: BlstCrypto,
        data_dir: &Path,
    ) -> (Mempool, DisseminationManager) {
        let lanes = shared_lanes_storage(data_dir).unwrap();
        let bus = MempoolBusClient::new_from_bus(shared_bus.new_handle()).await;

        let conf = Conf::new(
            vec![],
            Some(data_dir.to_string_lossy().to_string()),
            Some(false),
        );
        let ctx = crate::model::SharedRunContext {
            config: conf.unwrap_or_default().into(),
            api: Arc::new(BuildApiContextInner::default()),
            crypto: crypto.clone().into(),
            start_height: None,
        };

        // TODO: split module from functionality?
        let dissemination_manager =
            super::dissemination::DisseminationManager::build(shared_bus.new_handle(), ctx)
                .await
                .expect("Failed to build DisseminationManager");

        // Initialize Mempool
        (
            Mempool {
                bus,
                file: None,
                conf: SharedConf::default(),
                crypto: Arc::new(crypto),
                metrics: MempoolMetrics::global("id".to_string()),
                lanes,
                inner: MempoolStore::default(),
            },
            dissemination_manager,
        )
    }

    pub async fn new_with_shared_bus(
        name: &str,
        shared_bus: &SharedMessageBus,
        crypto: BlstCrypto,
    ) -> Self {
        let out_receiver = get_receiver::<OutboundMessage>(shared_bus).await;
        let mempool_event_receiver = get_receiver::<MempoolBlockEvent>(shared_bus).await;
        let mempool_status_event_receiver = get_receiver::<MempoolStatusEvent>(shared_bus).await;
        let dissemination_event_receiver = get_receiver::<DisseminationEvent>(shared_bus).await;

        let data_dir = tempfile::tempdir().unwrap().keep();
        let (mempool, dissemination_manager) =
            Self::build_mempool(shared_bus, crypto, &data_dir).await;

        MempoolTestCtx {
            name: name.to_string(),
            out_receiver,
            mempool_event_receiver,
            mempool_status_event_receiver,
            dissemination_event_receiver,
            mempool,
            dissemination_manager,
            data_dir,
        }
    }

    pub async fn new(name: &str) -> Self {
        let crypto = BlstCrypto::new(name).unwrap();
        let shared_bus = SharedMessageBus::new(BusMetrics::global("global".to_string()));
        Self::new_with_shared_bus(name, &shared_bus, crypto).await
    }

    pub async fn setup_node(&mut self, cryptos: &[BlstCrypto]) {
        for other_crypto in cryptos.iter() {
            self.add_trusted_validator(other_crypto.validator_pubkey())
                .await;
        }
    }

    pub fn own_lane(&self) -> LaneId {
        self.mempool.own_lane_id()
    }

    pub fn validator_pubkey(&self) -> &ValidatorPublicKey {
        self.mempool.crypto.validator_pubkey()
    }

    pub fn set_ready_to_create_dps(&mut self) {
        self.mempool.ready_to_create_dps = true;
    }

    pub async fn add_trusted_validator(&mut self, pubkey: &ValidatorPublicKey) {
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

        self.dissemination_manager
            .on_consensus_event(ConsensusEvent::CommitConsensusProposal(
                CommittedConsensusProposal {
                    staking: self.mempool.staking.clone(),
                    consensus_proposal: ConsensusProposal::default(),
                    certificate: AggregateSignature::default(),
                },
            ))
            .expect("update dissemination staking");
        self.process_dissemination_events()
            .await
            .expect("process dissemination events");
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

    pub async fn timer_tick(&mut self) -> Result<()> {
        let Ok(true) = self.mempool.prepare_new_data_proposal() else {
            debug!("No new data proposal to prepare");
            return self.disseminate_owned_lanes().await;
        };

        let (lane_id, _dp_hash) = self
            .mempool
            .own_data_proposal_in_preparation
            .join_next()
            .await
            .context("join next data proposal in preparation")??;

        self.mempool.resume_new_data_proposal(lane_id).await?;

        self.disseminate_owned_lanes().await
    }

    pub async fn disseminate_owned_lanes(&mut self) -> Result<()> {
        self.dissemination_manager
            .add_owned_lane(self.mempool.own_lane_id());
        self.dissemination_manager.redisseminate_owned_lanes().await
    }

    pub fn maybe_disseminate_dp(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<()> {
        self.dissemination_manager
            .maybe_disseminate_dp(lane_id, dp_hash)
    }

    pub async fn handle_poda_update(&mut self, net_message: MsgWithHeader<MempoolNetMessage>) {
        self.mempool
            .handle_net_message(net_message)
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
            .await
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
                OutboundMessage::BroadcastMessage(net_msg)
                | OutboundMessage::BroadcastMessageOnlyFor(_, net_msg) => {
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
            .handle_net_message(msg.clone())
            .await
            .expect("should handle net msg");
    }

    pub fn current_hash(&self, lane_id: &LaneId) -> Option<DataProposalHash> {
        self.mempool.lanes.get_lane_hash_tip(lane_id)
    }

    pub fn current_size_of(&self, lane_id: &LaneId) -> Option<LaneBytesSize> {
        self.mempool.lanes.get_lane_size_tip(lane_id)
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
        let lane_id = LaneId::new(self.validator_pubkey().clone());
        self.current_size_of(&lane_id)
    }

    pub fn push_data_proposal(&mut self, dp: DataProposal) {
        let lane_id = LaneId::new(self.validator_pubkey().clone());

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
                        cached_poda: None,
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
            .handle_api_message(RestApiMessage::NewTx {
                tx: tx.clone(),
                lane_suffix: None,
            })
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
        let (dp_hash, cumul_size) = self.mempool.lanes.store_data_proposal(
            &self.mempool.crypto,
            &LaneId::new(self.mempool.crypto.validator_pubkey().clone()),
            dp,
        )?;
        let lane_id = LaneId::new(self.mempool.crypto.validator_pubkey().clone());
        self.mempool
            .send_dissemination_event(DisseminationEvent::NewDpCreated {
                lane_id: lane_id.clone(),
                data_proposal_hash: dp_hash.clone(),
            })?;
        self.mempool
            .send_dissemination_event(DisseminationEvent::DpStored {
                lane_id,
                data_proposal_hash: dp_hash,
                cumul_size,
            })?;
        Ok(())
    }

    pub async fn process_dissemination_events(&mut self) -> Result<()> {
        loop {
            match self.dissemination_event_receiver.try_recv() {
                Ok(event) => {
                    self.dissemination_manager
                        .on_event(event.into_message())
                        .await?
                }
                Err(TryRecvError::Empty) | Err(TryRecvError::Closed) => break,
                Err(TryRecvError::Lagged(_)) => continue,
            }
        }
        Ok(())
    }

    pub async fn process_sync(&mut self) -> Result<()> {
        self.process_dissemination_events().await?;
        self.dissemination_manager
            .process_sync_requests_and_replies_for_test()
            .await
    }

    pub async fn process_cut_with_dp(
        &mut self,
        leader: &ValidatorPublicKey,
        dp_hash: &DataProposalHash,
        cumul_size: LaneBytesSize,
        slot: u64,
    ) -> Result<Cut> {
        let cut = vec![(
            LaneId::new(leader.clone()),
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
    lane_id: LaneId,
    hash: DataProposalHash,
    size: LaneBytesSize,
) -> Result<MempoolNetMessage> {
    Ok(MempoolNetMessage::DataVote(
        lane_id,
        crypto.sign((hash, size))?,
    ))
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
    let lane_id = LaneId::new(pubkey2.clone());
    ctx.add_trusted_validator(pubkey2).await;

    ctx.handle_consensus_event(ConsensusProposal {
        slot: 1,
        cut: vec![(
            lane_id.clone(),
            DataProposalHash("dp_hash_in_cut".to_owned()),
            LaneBytesSize::default(),
            PoDA::default(),
        )],
        ..ConsensusProposal::default()
    })
    .await;

    ctx.process_sync().await?;
    // Assert that we send a SyncRequest
    match ctx
        .assert_send(crypto2.validator_pubkey(), "SyncRequest")
        .await
        .msg
    {
        MempoolNetMessage::SyncRequest(req_lane_id, from, to) => {
            assert_eq!(req_lane_id, lane_id);
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
    let (..) = ctx.last_lane_entry(&LaneId::new(ctx.validator_pubkey().clone()));
    let lane_id = ctx.mempool.own_lane_id();

    // Add new validator
    let crypto2 = BlstCrypto::new("2").unwrap();
    ctx.add_trusted_validator(crypto2.validator_pubkey()).await;

    for _ in 1..5 {
        let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncRequest(
            lane_id.clone(),
            None,
            Some(data_proposal.hashed()),
        ))?;

        ctx.mempool
            .handle_net_message(signed_msg)
            .await
            .expect("should handle net message");
    }

    ctx.process_sync().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    for _ in 1..5 {
        let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncRequest(
            lane_id.clone(),
            None,
            Some(data_proposal.hashed()),
        ))?;

        ctx.mempool
            .handle_net_message(signed_msg)
            .await
            .expect("should handle net message");
    }
    ctx.process_sync().await?;

    let mut replies = Vec::new();
    while let Ok(outbound) = ctx.out_receiver.try_recv() {
        match outbound.into_message() {
            OutboundMessage::SendMessage { validator_id, msg } => {
                assert_eq!(&validator_id, crypto2.validator_pubkey());
                match msg {
                    NetMessage::MempoolMessage(msg) => match msg.msg {
                        MempoolNetMessage::SyncReply(_, _signatures, dp) => replies.push(dp),
                        other => panic!("Expected SyncReply message, got {other:?}"),
                    },
                    other => panic!("Expected mempool message, got {other:?}"),
                }
            }
            OutboundMessage::BroadcastMessage(_) | OutboundMessage::BroadcastMessageOnlyFor(..) => {
                continue;
            }
        }
    }

    assert!(!replies.is_empty(), "Expected at least one SyncReply");
    for reply in replies {
        assert_eq!(reply, data_proposal);
    }

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
    let (..) = ctx.last_lane_entry(&LaneId::new(ctx.validator_pubkey().clone()));
    let lane_id = ctx.mempool.own_lane_id();

    // Add new validator
    let crypto2 = BlstCrypto::new("2").unwrap();
    ctx.add_trusted_validator(crypto2.validator_pubkey()).await;

    // Sync request for the interval up to data proposal
    for _ in 1..3 {
        let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncRequest(
            lane_id.clone(),
            None,
            Some(data_proposal.hashed()),
        ))?;

        ctx.mempool
            .handle_net_message(signed_msg)
            .await
            .expect("should handle net message");
    }

    ctx.process_sync().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Sync request for the whole interval
    for _ in 1..3 {
        let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncRequest(
            lane_id.clone(),
            None,
            Some(data_proposal2.hashed()),
        ))?;

        ctx.mempool
            .handle_net_message(signed_msg)
            .await
            .expect("should handle net message");
    }

    ctx.process_sync().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Sync request for the whole interval
    for _ in 1..3 {
        let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncRequest(
            lane_id.clone(),
            None,
            None,
        ))?;

        ctx.mempool
            .handle_net_message(signed_msg)
            .await
            .expect("should handle net message");
    }
    ctx.process_sync().await?;

    let mut replies = std::collections::HashSet::new();
    while let Ok(outbound) = ctx.out_receiver.try_recv() {
        match outbound.into_message() {
            OutboundMessage::SendMessage { validator_id, msg } => {
                assert_eq!(&validator_id, crypto2.validator_pubkey());
                match msg {
                    NetMessage::MempoolMessage(msg) => match msg.msg {
                        MempoolNetMessage::SyncReply(_, _signatures, dp) => {
                            replies.insert(dp.hashed());
                        }
                        other => panic!("Expected SyncReply message, got {other:?}"),
                    },
                    other => panic!("Expected mempool message, got {other:?}"),
                }
            }
            OutboundMessage::BroadcastMessage(_) | OutboundMessage::BroadcastMessageOnlyFor(..) => {
                continue;
            }
        }
    }

    assert!(replies.contains(&data_proposal.hashed()));
    assert!(replies.contains(&data_proposal2.hashed()));
    assert!(ctx.out_receiver.is_empty());
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_sync_reply_buffered_without_buc() -> Result<()> {
    let mut ctx = MempoolTestCtx::new("mempool").await;

    let register_tx = make_register_contract_tx(ContractName::new("test1"));
    let dp1 = ctx.create_data_proposal(None, std::slice::from_ref(&register_tx));
    let cumul_size1 = LaneBytesSize(dp1.estimate_size() as u64);
    let lane_id = ctx.mempool.own_lane_id();

    // Add new validator
    let crypto2 = BlstCrypto::new("2").unwrap();
    ctx.add_trusted_validator(crypto2.validator_pubkey()).await;

    // Sync reply arrives before BUC exists -> buffer it.
    let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncReply(
        lane_id.clone(),
        vec![ctx
            .mempool
            .crypto
            .sign((dp1.hashed(), cumul_size1))
            .expect("should sign")],
        dp1.clone(),
    ))?;
    let handle = ctx.mempool.handle_net_message(signed_msg).await;
    assert_ok!(handle, "Should handle net message");
    assert!(ctx
        .mempool
        .inner
        .buffered_entries
        .get(&lane_id)
        .and_then(|m| m.get(&dp1.hashed()))
        .is_some());
    assert!(!ctx.mempool.lanes.contains(&lane_id, &dp1.hashed()));

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_data_vote_invalid_signature_rejected() -> Result<()> {
    let mut ctx = MempoolTestCtx::new("mempool").await;

    let crypto2 = BlstCrypto::new("2").unwrap();
    let lane_id = ctx.mempool.own_lane_id().clone();
    let valid = crypto2.sign((DataProposalHash("hash-a".to_string()), LaneBytesSize(1)))?;
    let invalid = SignedByValidator {
        msg: (DataProposalHash("hash-b".to_string()), LaneBytesSize(1)),
        signature: valid.signature,
    };

    let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::DataVote(lane_id, invalid))?;
    let handle = ctx.mempool.handle_net_message(signed_msg).await;
    assert!(handle.is_err(), "Expected invalid signature to be rejected");

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_sync_reply_fills_buffered_chain_and_preserves_unrelated_buffer() -> Result<()> {
    let mut ctx = MempoolTestCtx::new("mempool").await;

    let register_tx = make_register_contract_tx(ContractName::new("test1"));
    let dp1 = ctx.create_data_proposal(None, std::slice::from_ref(&register_tx));
    let dp2 = ctx.create_data_proposal(Some(dp1.hashed()), std::slice::from_ref(&register_tx));
    let dp3 = ctx.create_data_proposal(Some(dp2.hashed()), std::slice::from_ref(&register_tx));
    let register_tx2 = make_register_contract_tx(ContractName::new("test2"));
    let dp_other = ctx.create_data_proposal(None, std::slice::from_ref(&register_tx2));

    let dp1_size = LaneBytesSize(dp1.estimate_size() as u64);
    let dp2_size = LaneBytesSize(dp2.estimate_size() as u64);
    let dp3_size = LaneBytesSize(dp3.estimate_size() as u64);
    let cumul_size1 = dp1_size;
    let cumul_size2 = LaneBytesSize(dp1_size.0 + dp2_size.0);
    let cumul_size3 = LaneBytesSize(dp1_size.0 + dp2_size.0 + dp3_size.0);
    let lane_id = ctx.mempool.own_lane_id();

    // Add new validator
    let crypto2 = BlstCrypto::new("2").unwrap();
    ctx.add_trusted_validator(crypto2.validator_pubkey()).await;

    // Buffer dp1 and dp2 (no BUC yet).
    for (dp, size) in [(dp1.clone(), cumul_size1), (dp2.clone(), cumul_size2)] {
        let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncReply(
            lane_id.clone(),
            vec![ctx
                .mempool
                .crypto
                .sign((dp.hashed(), size))
                .expect("should sign")],
            dp,
        ))?;
        let handle = ctx.mempool.handle_net_message(signed_msg).await;
        assert_ok!(handle, "Should handle net message");
    }

    // Buffer unrelated entry that should remain.
    let other_size = LaneBytesSize(dp_other.estimate_size() as u64);
    let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncReply(
        lane_id.clone(),
        vec![ctx
            .mempool
            .crypto
            .sign((dp_other.hashed(), other_size))
            .expect("should sign")],
        dp_other.clone(),
    ))?;
    let handle = ctx.mempool.handle_net_message(signed_msg).await;
    assert_ok!(handle, "Should handle net message");

    let mut holes_tops = std::collections::HashMap::new();
    holes_tops.insert(lane_id.clone(), (dp3.hashed(), cumul_size3));
    ctx.mempool.inner.blocks_under_contruction.push_back(
        crate::mempool::block_construction::BlockUnderConstruction {
            from: None,
            ccp: CommittedConsensusProposal {
                consensus_proposal: ConsensusProposal {
                    cut: vec![(lane_id.clone(), dp3.hashed(), cumul_size3, PoDA::default())],
                    ..ConsensusProposal::default()
                },
                staking: Staking::default(),
                certificate: AggregateSignature::default(),
            },
            holes_tops,
            holes_materialized: true,
        },
    );

    // Reply for dp3 should fill dp3 -> dp2 -> dp1 from buffer.
    let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncReply(
        lane_id.clone(),
        vec![ctx
            .mempool
            .crypto
            .sign((dp3.hashed(), cumul_size3))
            .expect("should sign")],
        dp3.clone(),
    ))?;
    let handle = ctx.mempool.handle_net_message(signed_msg).await;
    assert_ok!(handle, "Should handle net message");

    assert!(ctx.mempool.lanes.contains(&lane_id, &dp1.hashed()));
    assert!(ctx.mempool.lanes.contains(&lane_id, &dp2.hashed()));
    assert!(ctx.mempool.lanes.contains(&lane_id, &dp3.hashed()));

    let metadata = ctx
        .mempool
        .lanes
        .get_metadata_by_hash(&lane_id, &dp1.hashed())?
        .expect("Expected stored metadata");
    assert_eq!(metadata.parent_data_proposal_hash, None);
    assert_eq!(metadata.cumul_size, cumul_size1);

    let metadata = ctx
        .mempool
        .lanes
        .get_metadata_by_hash(&lane_id, &dp2.hashed())?
        .expect("Expected stored metadata");
    assert_eq!(metadata.parent_data_proposal_hash, Some(dp1.hashed()));
    assert_eq!(metadata.cumul_size, cumul_size2);

    let metadata = ctx
        .mempool
        .lanes
        .get_metadata_by_hash(&lane_id, &dp3.hashed())?
        .expect("Expected stored metadata");
    assert_eq!(metadata.parent_data_proposal_hash, Some(dp2.hashed()));
    assert_eq!(metadata.cumul_size, cumul_size3);

    // Unrelated buffered entry remains.
    assert!(ctx
        .mempool
        .inner
        .buffered_entries
        .get(&lane_id)
        .and_then(|m| m.get(&dp_other.hashed()))
        .is_some());

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_sync_reply_materialize_holes_consumes_buffered_latest_cut() -> Result<()> {
    let mut ctx = MempoolTestCtx::new("mempool").await;

    let register_tx = make_register_contract_tx(ContractName::new("test1"));
    let dp1 = ctx.create_data_proposal(None, std::slice::from_ref(&register_tx));
    let dp2 = ctx.create_data_proposal(Some(dp1.hashed()), std::slice::from_ref(&register_tx));
    let dp3 = ctx.create_data_proposal(Some(dp2.hashed()), std::slice::from_ref(&register_tx));

    let dp1_size = LaneBytesSize(dp1.estimate_size() as u64);
    let dp2_size = LaneBytesSize(dp2.estimate_size() as u64);
    let dp3_size = LaneBytesSize(dp3.estimate_size() as u64);
    let cumul_size1 = dp1_size;
    let cumul_size2 = LaneBytesSize(dp1_size.0 + dp2_size.0);
    let cumul_size3 = LaneBytesSize(dp1_size.0 + dp2_size.0 + dp3_size.0);
    let lane_id = ctx.mempool.own_lane_id();

    // Add new validator
    let crypto2 = BlstCrypto::new("2").unwrap();
    ctx.add_trusted_validator(crypto2.validator_pubkey()).await;

    // Buffer all entries before any cut/BUC.
    for (dp, size) in [
        (dp1.clone(), cumul_size1),
        (dp2.clone(), cumul_size2),
        (dp3.clone(), cumul_size3),
    ] {
        let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncReply(
            lane_id.clone(),
            vec![ctx
                .mempool
                .crypto
                .sign((dp.hashed(), size))
                .expect("should sign")],
            dp,
        ))?;
        let handle = ctx.mempool.handle_net_message(signed_msg).await;
        assert_ok!(handle, "Should handle net message");
    }

    let mut buc = crate::mempool::block_construction::BlockUnderConstruction {
        from: None,
        ccp: CommittedConsensusProposal {
            consensus_proposal: ConsensusProposal {
                cut: vec![(lane_id.clone(), dp3.hashed(), cumul_size3, PoDA::default())],
                slot: 1,
                ..ConsensusProposal::default()
            },
            staking: Staking::default(),
            certificate: AggregateSignature::default(),
        },
        holes_tops: std::collections::HashMap::new(),
        holes_materialized: false,
    };

    let result = ctx.mempool.build_signed_block_and_emit(&mut buc).await;
    assert_ok!(
        result,
        "Should build signed block after consuming buffered entries"
    );

    assert!(ctx.mempool.lanes.contains(&lane_id, &dp1.hashed()));
    assert!(ctx.mempool.lanes.contains(&lane_id, &dp2.hashed()));
    assert!(ctx.mempool.lanes.contains(&lane_id, &dp3.hashed()));

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
            lane_id.clone(),
            None,
            Some(dp2_hash.clone()),
            crypto.validator_pubkey().clone(),
        )
        .await?;

    ctx.process_sync().await?;

    // Verify that only dp2 was sent in the sync reply
    match ctx
        .assert_send(&crypto.validator_pubkey().clone(), "SyncReply")
        .await
        .msg
    {
        MempoolNetMessage::SyncReply(reply_lane_id, signatures, dp) => {
            assert_eq!(reply_lane_id, lane_id);
            assert_eq!(dp, dp2);
            assert_eq!(signatures.len(), 1);
            assert_eq!(signatures[0].msg.0, dp2_hash);
            assert_eq!(signatures[0].msg.1, dp2_size);
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

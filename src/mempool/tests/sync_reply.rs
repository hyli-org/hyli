use super::*;

use anyhow::Result;
use hyli_crypto::BlstCrypto;
use hyli_model::{AggregateSignature, LaneBytesSize};
use hyli_net::clock::TimestampMsClock;
use std::time::Duration;

#[test_log::test(tokio::test)]
async fn throttles_outbound_sync_requests_during_backoff() -> Result<()> {
    let mut ctx = MempoolTestCtx::new("mempool").await;

    let crypto2 = BlstCrypto::new("2")?;
    ctx.add_trusted_validator(crypto2.validator_pubkey()).await;

    let lane_id = LaneId::new(crypto2.validator_pubkey().clone());
    let dp_hash = DataProposalHash::from("missing_dp");
    ctx.dissemination_manager.set_request_backoff_for_test(
        lane_id.clone(),
        dp_hash,
        TimestampMsClock::now(),
        Duration::from_secs(60),
        0,
    );
    ctx.process_sync().await?;

    assert!(
        tokio::time::timeout(Duration::from_millis(200), ctx.out_receiver.recv())
            .await
            .is_err()
    );

    Ok(())
}

#[test_log::test(tokio::test)]
async fn sends_request_after_backoff_window_expires() -> Result<()> {
    let mut ctx = MempoolTestCtx::new("mempool").await;

    let crypto2 = BlstCrypto::new("2")?;
    ctx.add_trusted_validator(crypto2.validator_pubkey()).await;

    let lane_id = LaneId::new(crypto2.validator_pubkey().clone());
    let dp_hash = DataProposalHash::from("missing_dp");
    ctx.dissemination_manager.set_request_backoff_for_test(
        lane_id.clone(),
        dp_hash.clone(),
        TimestampMsClock::now() - Duration::from_secs(60),
        Duration::from_millis(10),
        0,
    );

    ctx.process_sync().await?;

    match ctx
        .assert_send(crypto2.validator_pubkey(), "SyncRequest")
        .await
        .msg
    {
        MempoolNetMessage::SyncRequest(req_lane_id, from, to) => {
            assert_eq!(req_lane_id, lane_id);
            assert_eq!(from, None);
            assert_eq!(to, Some(dp_hash));
        }
        other => ::core::panic!("Expected SyncRequest message, got {other:?}"),
    };

    Ok(())
}

#[test_log::test(tokio::test)]
async fn prefers_ccp_signers_for_sync_requests() -> Result<()> {
    let mut ctx = MempoolTestCtx::new("mempool").await;

    let crypto2 = BlstCrypto::new("2")?;
    let crypto3 = BlstCrypto::new("3")?;
    let crypto4 = BlstCrypto::new("4")?;
    ctx.add_trusted_validator(crypto2.validator_pubkey()).await;
    ctx.add_trusted_validator(crypto3.validator_pubkey()).await;
    ctx.add_trusted_validator(crypto4.validator_pubkey()).await;

    let peers: Vec<ValidatorPublicKey> = ctx
        .mempool
        .staking
        .bonded()
        .iter()
        .filter(|pubkey| *pubkey != ctx.validator_pubkey())
        .cloned()
        .collect();
    assert!(peers.len() >= 3);

    let strong_peer = peers[1].clone();
    assert_ne!(peers[0], strong_peer);

    let lane_id = LaneId::new(peers[0].clone());
    let dp_hash = DataProposalHash::from("missing_dp");
    ctx.dissemination_manager.set_request_backoff_for_test(
        lane_id.clone(),
        dp_hash.clone(),
        TimestampMsClock::now() - Duration::from_secs(60),
        Duration::from_millis(10),
        peers.len(),
    );

    ctx.dissemination_manager
        .on_consensus_event(ConsensusEvent::CommitConsensusProposal(
            CommittedConsensusProposal {
                staking: ctx.mempool.staking.clone(),
                consensus_proposal: ConsensusProposal {
                    slot: 1,
                    cut: vec![(
                        lane_id.clone(),
                        dp_hash.clone(),
                        LaneBytesSize(0),
                        AggregateSignature {
                            validators: vec![strong_peer.clone()],
                            ..AggregateSignature::default()
                        },
                    )],
                    staking_actions: vec![],
                    timestamp: TimestampMs(777),
                    parent_hash: ConsensusProposalHash::from("test"),
                },
                certificate: AggregateSignature::default(),
            },
        ))
        .expect("update dissemination cut");

    ctx.process_sync().await?;

    match ctx.assert_send(&strong_peer, "SyncRequest").await.msg {
        MempoolNetMessage::SyncRequest(req_lane_id, from, to) => {
            assert_eq!(req_lane_id, lane_id);
            assert_eq!(from, None);
            assert_eq!(to, Some(dp_hash));
        }
        other => ::core::panic!("Expected SyncRequest message, got {other:?}"),
    };

    Ok(())
}

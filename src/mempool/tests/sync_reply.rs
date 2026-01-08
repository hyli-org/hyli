use super::*;

use anyhow::Result;
use hyli_crypto::BlstCrypto;
use hyli_net::clock::TimestampMsClock;
use std::time::Duration;

#[test_log::test(tokio::test)]
async fn throttles_outbound_sync_requests_during_backoff() -> Result<()> {
    let mut ctx = MempoolTestCtx::new("mempool").await;

    let crypto2 = BlstCrypto::new("2")?;
    ctx.add_trusted_validator(crypto2.validator_pubkey()).await;

    let lane_id = LaneId(crypto2.validator_pubkey().clone());
    let dp_hash = DataProposalHash("missing_dp".to_string());
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

    let lane_id = LaneId(crypto2.validator_pubkey().clone());
    let dp_hash = DataProposalHash("missing_dp".to_string());
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

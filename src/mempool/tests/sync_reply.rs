use super::*;

use anyhow::Result;
use hyli_crypto::BlstCrypto;
use hyli_net::clock::TimestampMsClock;
use std::time::Duration;

#[test_log::test(tokio::test)]
async fn throttles_replies_during_backoff() -> Result<()> {
    let mut ctx = MempoolTestCtx::new("mempool").await;

    let register_tx = make_register_contract_tx(ContractName::new("test1"));
    let data_proposal = ctx.create_data_proposal(None, std::slice::from_ref(&register_tx));
    ctx.process_new_data_proposal(data_proposal.clone())?;
    ctx.process_dissemination_events().await?;
    while ctx.out_receiver.try_recv().is_ok() {}

    let crypto2 = BlstCrypto::new("2")?;
    ctx.add_trusted_validator(crypto2.validator_pubkey()).await;

    let lane_id = ctx.mempool.own_lane_id();
    ctx.dissemination_manager.set_sync_backoff_for_test(
        lane_id.clone(),
        crypto2.validator_pubkey().clone(),
        data_proposal.hashed(),
        TimestampMsClock::now(),
        Duration::from_secs(60),
    );

    let request = MempoolNetMessage::SyncRequest(
        lane_id.clone(),
        None,
        Some(data_proposal.hashed()),
    );
    let signed_msg = crypto2.sign_msg_with_header(request)?;
    ctx.mempool.handle_net_message(signed_msg).await?;
    ctx.process_sync().await?;

    assert!(
        tokio::time::timeout(Duration::from_millis(200), ctx.out_receiver.recv())
            .await
            .is_err()
    );

    Ok(())
}

#[test_log::test(tokio::test)]
async fn sends_again_after_backoff_window_expires() -> Result<()> {
    let mut ctx = MempoolTestCtx::new("mempool").await;

    let register_tx = make_register_contract_tx(ContractName::new("test1"));
    let data_proposal = ctx.create_data_proposal(None, std::slice::from_ref(&register_tx));
    ctx.process_new_data_proposal(data_proposal.clone())?;
    ctx.process_dissemination_events().await?;
    while ctx.out_receiver.try_recv().is_ok() {}

    let crypto2 = BlstCrypto::new("2")?;
    ctx.add_trusted_validator(crypto2.validator_pubkey()).await;

    let lane_id = ctx.mempool.own_lane_id();
    ctx.dissemination_manager.set_sync_backoff_for_test(
        lane_id.clone(),
        crypto2.validator_pubkey().clone(),
        data_proposal.hashed(),
        TimestampMsClock::now() - Duration::from_secs(60),
        Duration::from_millis(10),
    );

    let request = MempoolNetMessage::SyncRequest(
        lane_id.clone(),
        None,
        Some(data_proposal.hashed()),
    );
    let signed_msg = crypto2.sign_msg_with_header(request)?;
    ctx.mempool.handle_net_message(signed_msg).await?;
    ctx.process_sync().await?;

    match ctx
        .assert_send(crypto2.validator_pubkey(), "SyncReply")
        .await
        .msg
    {
        MempoolNetMessage::SyncReply(_, _metadata, data_proposal_r) => {
            assert_eq!(data_proposal_r, data_proposal);
        }
        other => ::core::panic!("Expected SyncReply message, got {other:?}"),
    };

    Ok(())
}

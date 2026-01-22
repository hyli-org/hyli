use crate::consensus::{test::ConsensusTestCtx, ConsensusNetMessage};
use hyli_crypto::BlstCrypto;
use hyli_model::utils::TimestampMs;
use tracing::info;

use super::autobahn_testing::AutobahnTestCtx;
use super::autobahn_testing_macros::*;

#[test_log::test(tokio::test)]
async fn test_buffer_and_sync_reply_single() {
    // This scenario tests the buffering & sync-request - sync-reply logic when
    // missing a single slot.
    // In this case, we see the prepare for round 5, miss the commit,
    // miss the prepare/commit for round 6
    // See prepare for round 7.

    // node 4 got disconnected for a slot
    let (mut node1, mut node2, mut node3, mut node4) = build_nodes!(4).await;

    ConsensusTestCtx::setup_for_round(
        &mut [
            &mut node1.consensus_ctx,
            &mut node2.consensus_ctx,
            &mut node3.consensus_ctx,
            &mut node4.consensus_ctx,
        ],
        5,
        0,
    );

    // Slot 5 starts, all nodes receive the prepare
    node1
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    broadcast! {
        description: "Confirm - Node4 disconnected",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node2.consensus_ctx, node3.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    broadcast! {
        description: "Commit - Node4 still disconnected",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };

    // Slot 6 starts with new leader with node4 disconnected
    node2
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;

    simple_commit_round! {
        leader: node2.consensus_ctx,
        followers: [node1.consensus_ctx, node3.consensus_ctx]
    };

    // Slot 7 starts with new leader but node4 is back online
    node3
        .start_round_with_cut_from_mempool(TimestampMs(3000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node3.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node1.consensus_ctx, node2.consensus_ctx], to: node3.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    let confirm = node3.consensus_ctx.assert_broadcast("Confirm").await;

    send! {
        description: "SyncRequest - Node4 ask for missed proposal Slot 4",
        from: [node4.consensus_ctx], to: node3.consensus_ctx,
        message_matches: ConsensusNetMessage::SyncRequest(_)
    };

    send! {
        description: "SyncReply - Node 3 replies with proposal Slot 4",
        from: [node3.consensus_ctx], to: node4.consensus_ctx,
        message_matches: ConsensusNetMessage::SyncReply(_)
    };

    send! {
        description: "PrepareVote - Node4 votes on slot 5",
        from: [node4.consensus_ctx], to: node3.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    node1
        .consensus_ctx
        .handle_msg(
            &confirm,
            "[handling broadcast message from: node3 at: node1] Confirm",
        )
        .await;
    node2
        .consensus_ctx
        .handle_msg(
            &confirm,
            "[handling broadcast message from: node3 at: node2] Confirm",
        )
        .await;
    node4
        .consensus_ctx
        .handle_msg(
            &confirm,
            "[handling broadcast message from: node3 at: node4] Confirm",
        )
        .await;

    send! {
        description: "ConfirmAck",
        from: [node1.consensus_ctx, node2.consensus_ctx, node4.consensus_ctx], to: node3.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    broadcast! {
        description: "Commit",
        from: node3.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };

    // Slot 9 starts with node4 as leader
    node4
        .start_round_with_cut_from_mempool(TimestampMs(4000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node4.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    assert_eq!(node1.consensus_ctx.slot(), 8);
    assert_eq!(node2.consensus_ctx.slot(), 8);
    assert_eq!(node3.consensus_ctx.slot(), 8);
    assert_eq!(node4.consensus_ctx.slot(), 8);
}

#[test_log::test(tokio::test)]
async fn test_buffer_and_sync_reply_multiple() {
    // This scenario tests the buffering & sync-request - sync-reply logic when
    // missing multiple slots.
    // In this case, we see the prepare for round 5, miss the commit,
    // miss the prepare/commit for round 6 & 7
    // See prepare for round 8.
    // (we fail to send prepare for R8V0, but that's OK)

    // node 4 got disconnected for a slot
    let (mut node1, mut node2, mut node3, mut node4) = build_nodes!(4).await;

    ConsensusTestCtx::setup_for_round(
        &mut [
            &mut node1.consensus_ctx,
            &mut node2.consensus_ctx,
            &mut node3.consensus_ctx,
            &mut node4.consensus_ctx,
        ],
        5,
        0,
    );

    // Slot 5 starts, all nodes receive the prepare
    node1
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    broadcast! {
        description: "Confirm - Node4 disconnected",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node2.consensus_ctx, node3.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    broadcast! {
        description: "Commit - Node4 still disconnected",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };

    // Slot 6 starts with new leader with node4 disconnected
    node2
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;

    simple_commit_round! {
        leader: node2.consensus_ctx,
        followers: [node1.consensus_ctx, node3.consensus_ctx]
    };

    // Slot 7 starts with new leader
    node3
        .start_round_with_cut_from_mempool(TimestampMs(3000))
        .await;

    simple_commit_round! {
        leader: node3.consensus_ctx,
        followers: [node2.consensus_ctx, node1.consensus_ctx]
    };

    // Timeout
    ConsensusTestCtx::timeout(&mut [
        &mut node1.consensus_ctx,
        &mut node2.consensus_ctx,
        &mut node3.consensus_ctx,
    ])
    .await;

    broadcast! {
        description: "Timeout",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Timeout",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Timeout",
        from: node3.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };

    // Node 1 is leader for R8v1
    node1
        .start_round_with_cut_from_mempool(TimestampMs(4000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    // Nodes 2 votes (I skip 3 to not commit)
    send! {
        description: "PrepareVote",
        from: [node2.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    // At this point, node 4 will request the parent, reply.
    send! {
        description: "SyncRequest - Ask from Node4",
        from: [node4.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::SyncRequest(_)
    };

    send! {
        description: "SyncReply - Reply with prepare",
        from: [node1.consensus_ctx], to: node4.consensus_ctx,
        message_matches: ConsensusNetMessage::SyncReply(_)
    };
    // Now we request the parent of the parent
    send! {
        description: "SyncRequest - Ask from Node4",
        from: [node4.consensus_ctx], to: node3.consensus_ctx,
        message_matches: ConsensusNetMessage::SyncRequest(_)
    };

    send! {
        description: "SyncReply - Reply with prepare",
        from: [node3.consensus_ctx], to: node4.consensus_ctx,
        message_matches: ConsensusNetMessage::SyncReply(_)
    };

    // Here we vote so the prepare is acked.
    send! {
        description: "PrepareVote",
        from: [node4.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    node1.consensus_ctx.assert_broadcast("Confirm").await;

    assert_eq!(node1.consensus_ctx.slot(), 8);
    assert_eq!(node2.consensus_ctx.slot(), 8);
    assert_eq!(node3.consensus_ctx.slot(), 8);
    assert_eq!(node4.consensus_ctx.slot(), 8);
}

#[test_log::test(tokio::test)]
async fn test_buffer_and_sync_reply_multiple_alt() {
    // This scenario tests the buffering & sync-request - sync-reply logic when
    // missing multiple slots.
    // In this case, we do not see the prepare for round 5, but we are at round 5 (simulating we saw 4)
    // miss the prepare/commit for round 6
    // See prepare for round 7.

    // node 4 got disconnected for a slot
    let (mut node1, mut node2, mut node3, mut node4) = build_nodes!(4).await;

    ConsensusTestCtx::setup_for_round(
        &mut [
            &mut node1.consensus_ctx,
            &mut node2.consensus_ctx,
            &mut node3.consensus_ctx,
            &mut node4.consensus_ctx,
        ],
        5,
        0,
    );

    // Slot 5 starts, all nodes receive the prepare with node4 disconnected
    node1
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    simple_commit_round! {
        leader: node1.consensus_ctx,
        followers: [node3.consensus_ctx, node2.consensus_ctx]
    };

    // Slot 6 starts with new leader with node4 disconnected
    node2
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;

    simple_commit_round! {
        leader: node2.consensus_ctx,
        followers: [node1.consensus_ctx, node3.consensus_ctx]
    };

    // Slot 7 starts with new leader but node4 is back online
    node3
        .start_round_with_cut_from_mempool(TimestampMs(3000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node3.consensus_ctx, to: [node2.consensus_ctx, node1.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    // At this point, node 4 will request the parent, reply.
    send! {
        description: "SyncRequest - Ask from Node4",
        from: [node4.consensus_ctx], to: node3.consensus_ctx,
        message_matches: ConsensusNetMessage::SyncRequest(_)
    };

    send! {
        description: "SyncReply - Reply with prepare",
        from: [node3.consensus_ctx], to: node4.consensus_ctx,
        message_matches: ConsensusNetMessage::SyncReply(_)
    };

    // Now we request the parent of the parent
    send! {
        description: "SyncRequest - Ask from Node4",
        from: [node4.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::SyncRequest(_)
    };

    send! {
        description: "SyncReply - Reply with prepare",
        from: [node2.consensus_ctx], to: node4.consensus_ctx,
        message_matches: ConsensusNetMessage::SyncReply(_)
    };

    assert_eq!(node1.consensus_ctx.slot(), 7);
    assert_eq!(node2.consensus_ctx.slot(), 7);
    assert_eq!(node3.consensus_ctx.slot(), 7);
    assert_eq!(node4.consensus_ctx.slot(), 7);
}

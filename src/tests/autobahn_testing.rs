//! This module is intended for "integration" testing of the consensus and other modules.

use super::autobahn_testing_macros::*;
use crate::{
    bus::{SharedMessageBus, bus_client, command_response::Query, dont_use_this::get_receiver},
    consensus::{
        ConsensusEvent, ConsensusNetMessage, TCKind, Ticket, TimeoutKind, test::ConsensusTestCtx,
    },
    mempool::{
        MempoolNetMessage, QueryNewCut,
        tests::{MempoolTestCtx, make_register_contract_tx},
    },
    model::*,
    p2p::{P2PCommand, network::OutboundMessage},
};
use assertables::assert_matches;
use futures::future::join_all;
use hyli_crypto::BlstCrypto;
use hyli_model::utils::TimestampMs;
use hyli_modules::handle_messages;
use hyli_modules::{
    bus::{BusEnvelope, dont_use_this::get_sender},
    node_state::NodeState,
};
use tracing::info;

bus_client!(
    pub struct AutobahnBusClient {
        receiver(Query<QueryNewCut, Cut>),
    }
);

pub struct AutobahnTestCtx {
    pub shared_bus: SharedMessageBus,
    pub consensus_ctx: ConsensusTestCtx,
    pub mempool_ctx: MempoolTestCtx,
}

impl AutobahnTestCtx {
    pub async fn new(name: &str, crypto: BlstCrypto) -> Self {
        let shared_bus = SharedMessageBus::new();
        let event_receiver = get_receiver::<ConsensusEvent>(&shared_bus).await;
        let p2p_receiver = get_receiver::<P2PCommand>(&shared_bus).await;
        let consensus_out_receiver = get_receiver::<OutboundMessage>(&shared_bus).await;
        let consensus = ConsensusTestCtx::build_consensus(&shared_bus, crypto.clone()).await;
        let mempool_ctx = MempoolTestCtx::new_with_shared_bus(name, &shared_bus, crypto).await;

        AutobahnTestCtx {
            shared_bus,
            consensus_ctx: ConsensusTestCtx {
                out_receiver: consensus_out_receiver,
                _event_receiver: event_receiver,
                _p2p_receiver: p2p_receiver,
                consensus,
                name: name.to_string(),
            },
            mempool_ctx,
        }
    }

    pub fn generate_cryptos(nb: usize) -> Vec<BlstCrypto> {
        let mut res: Vec<_> = (0..nb)
            .map(|i| {
                let crypto = BlstCrypto::new(&format!("node-{i}")).unwrap();
                info!("node {}: {}", i, crypto.validator_pubkey());
                crypto
            })
            .collect();
        // Staking currently sorts the bonded validators so do the same
        res.sort_by_key(|c| c.validator_pubkey().clone());
        res
    }

    /// Spawn a coroutine to answer the command response call of start_round, with the current of mempool
    pub async fn start_round_with_cut_from_mempool(&mut self, ts: TimestampMs) {
        let staking = self.consensus_ctx.staking();
        let latest_cut: Cut = self.mempool_ctx.gen_cut(&staking);

        let mut autobahn_client_bus =
            AutobahnBusClient::new_from_bus(self.shared_bus.new_handle()).await;

        tokio::spawn(async move {
            handle_messages! {
                on_bus autobahn_client_bus,
                listen<Query<QueryNewCut, Cut>> qnc => {
                    if let Ok(value) = qnc.take() {
                        value.answer(latest_cut.clone()).expect("error when injecting cut");

                        break;
                    }
                }
            }
        });

        self.consensus_ctx.start_round_at(ts).await;
    }

    /// Just start the round and wait for the timeout
    pub async fn start_round_with_last_seen_cut(&mut self, ts: TimestampMs) {
        self.consensus_ctx.start_round_at(ts).await;
    }
}

#[test_log::test(tokio::test)]
async fn autobahn_basic_flow() {
    let (mut node1, mut node2, mut node3, mut node4) = build_nodes!(4).await;

    let register_tx = make_register_contract_tx(ContractName::new("test1"));
    let register_tx_2 = make_register_contract_tx(ContractName::new("test2"));

    let dp = node1
        .mempool_ctx
        .create_data_proposal(None, &[register_tx, register_tx_2]);
    node1
        .mempool_ctx
        .process_new_data_proposal(dp.clone())
        .unwrap();
    node1
        .mempool_ctx
        .process_dissemination_events()
        .await
        .expect("process dissemination events");
    node1.mempool_ctx.timer_tick().await.unwrap();
    node1
        .mempool_ctx
        .process_dissemination_events()
        .await
        .expect("process dissemination events");

    let message = broadcast! {
        description: "Disseminate Tx",
        from: node1.mempool_ctx, to: [node2.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx],
        message_matches: MempoolNetMessage::DataProposal(..)
    };
    if let MempoolNetMessage::DataProposal(_, _, data, _) = &message.msg {
        assert_eq!(data.txs.len(), 2);
    }

    join_all(
        [
            &mut node2.mempool_ctx,
            &mut node3.mempool_ctx,
            &mut node4.mempool_ctx,
        ]
        .iter_mut()
        .map(|ctx| ctx.handle_processed_data_proposals()),
    )
    .await;

    broadcast! {
        description: "Disseminated Tx Vote",
        from: node2.mempool_ctx, to: [node1.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx],
        message_matches: MempoolNetMessage::DataVote(..)
    };
    broadcast! {
        description: "Disseminated Tx Vote",
        from: node3.mempool_ctx, to: [node1.mempool_ctx, node2.mempool_ctx, node4.mempool_ctx],
        message_matches: MempoolNetMessage::DataVote(..)
    };
    broadcast! {
        description: "Disseminated Tx Vote",
        from: node4.mempool_ctx, to: [node1.mempool_ctx, node2.mempool_ctx, node3.mempool_ctx],
        message_matches: MempoolNetMessage::DataVote(..)
    };

    let data_proposal_hash_node1 = node1
        .mempool_ctx
        .current_hash(&node1.mempool_ctx.own_lane())
        .expect("Current hash should be there");
    let node1_l_size = node1.mempool_ctx.current_size().unwrap();

    node1
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    let consensus_proposal;
    let staking = node1.consensus_ctx.staking();
    let f = staking.compute_f();

    broadcast! {
        description: "Prepare",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(cp, ..) => {
            consensus_proposal = cp.clone();
            let entry = cp
                .cut
                .iter()
                .find(|(lane_id, _hash, _size, _poda)| lane_id == &node1.mempool_ctx.own_lane())
                .expect("expected lane entry in cut");
            assert_eq!(entry.1, data_proposal_hash_node1);
            assert_eq!(entry.2, node1_l_size);
            let voting_power = staking.compute_voting_power(entry.3.validators.as_slice());
            assert!(voting_power > f);
        }
    };

    let (vote2, vote3, vote4) = send! {
        description: "PrepareVote",
        from: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    assert_matches!(
        vote2.msg,
        ConsensusNetMessage::PrepareVote(Signed { msg: (cp, _), .. })
        if cp == consensus_proposal.hashed()
    );
    assert_matches!(
        vote3.msg,
        ConsensusNetMessage::PrepareVote(Signed { msg: (cp, _), .. })
        if cp == consensus_proposal.hashed()
    );
    assert_matches!(
        vote4.msg,
        ConsensusNetMessage::PrepareVote(Signed { msg: (cp, _), .. })
        if cp == consensus_proposal.hashed()
    );

    broadcast! {
        description: "Confirm",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    let (vote2, vote3, vote4) = send! {
        description: "ConfirmAck",
        from: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    assert_matches!(
        vote2.msg,
        ConsensusNetMessage::ConfirmAck(Signed { msg: (cp, _), .. })
        if cp == consensus_proposal.hashed()
    );
    assert_matches!(
        vote3.msg,
        ConsensusNetMessage::ConfirmAck(Signed { msg: (cp, _), .. })
        if cp == consensus_proposal.hashed()
    );
    assert_matches!(
        vote4.msg,
        ConsensusNetMessage::ConfirmAck(Signed { msg: (cp, _), .. })
        if cp == consensus_proposal.hashed()
    );

    broadcast! {
        description: "Commit",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };
}

#[test_log::test(tokio::test)]
async fn mempool_broadcast_multiple_data_proposals() {
    let (mut node1, mut node2, mut node3, mut node4) = build_nodes!(4).await;

    // First data proposal

    let register_tx = make_register_contract_tx(ContractName::new("test1"));
    let register_tx_2 = make_register_contract_tx(ContractName::new("test2"));

    let dp = node1
        .mempool_ctx
        .create_data_proposal(None, &[register_tx, register_tx_2]);
    node1
        .mempool_ctx
        .process_new_data_proposal(dp.clone())
        .unwrap();
    node1.mempool_ctx.timer_tick().await.unwrap();

    broadcast! {
        description: "Disseminate Tx",
        from: node1.mempool_ctx, to: [node2.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx],
        message_matches: MempoolNetMessage::DataProposal(..)
    };

    join_all(
        [
            &mut node2.mempool_ctx,
            &mut node3.mempool_ctx,
            &mut node4.mempool_ctx,
        ]
        .iter_mut()
        .map(|ctx| ctx.handle_processed_data_proposals()),
    )
    .await;

    broadcast! {
        description: "Disseminated Tx Vote",
        from: node2.mempool_ctx, to: [node1.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx],
        message_matches: MempoolNetMessage::DataVote(..)
    };
    broadcast! {
        description: "Disseminated Tx Vote",
        from: node3.mempool_ctx, to: [node1.mempool_ctx, node2.mempool_ctx, node4.mempool_ctx],
        message_matches: MempoolNetMessage::DataVote(..)
    };
    broadcast! {
        description: "Disseminated Tx Vote",
        from: node4.mempool_ctx, to: [node1.mempool_ctx, node2.mempool_ctx, node3.mempool_ctx],
        message_matches: MempoolNetMessage::DataVote(..)
    };

    node1
        .mempool_ctx
        .process_dissemination_events()
        .await
        .expect("process dissemination events");

    // Second data proposal

    let register_tx = make_register_contract_tx(ContractName::new("test3"));
    let register_tx_2 = make_register_contract_tx(ContractName::new("test4"));

    let dp = node1
        .mempool_ctx
        .create_data_proposal(Some(dp.hashed()), &[register_tx, register_tx_2]);
    node1
        .mempool_ctx
        .process_new_data_proposal(dp.clone())
        .unwrap();
    node1.mempool_ctx.timer_tick().await.unwrap();

    broadcast! {
        description: "Disseminate Tx",
        from: node1.mempool_ctx, to: [node2.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx],
        message_matches: MempoolNetMessage::DataProposal(..)
    };

    join_all(
        [
            &mut node2.mempool_ctx,
            &mut node3.mempool_ctx,
            &mut node4.mempool_ctx,
        ]
        .iter_mut()
        .map(|ctx| ctx.handle_processed_data_proposals()),
    )
    .await;

    broadcast! {
        description: "Disseminated Tx Vote",
        from: node2.mempool_ctx, to: [node1.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx],
        message_matches: MempoolNetMessage::DataVote(..)
    };
    broadcast! {
        description: "Disseminated Tx Vote",
        from: node3.mempool_ctx, to: [node1.mempool_ctx, node2.mempool_ctx, node4.mempool_ctx],
        message_matches: MempoolNetMessage::DataVote(..)
    };
    broadcast! {
        description: "Disseminated Tx Vote",
        from: node4.mempool_ctx, to: [node1.mempool_ctx, node2.mempool_ctx, node3.mempool_ctx],
        message_matches: MempoolNetMessage::DataVote(..)
    };
}

#[test_log::test(tokio::test)]
async fn mempool_votes_before_data_proposal() {
    let (mut node1, mut node2, mut node3, mut node4) = build_nodes!(4).await;

    // First data proposal

    let register_tx = make_register_contract_tx(ContractName::new("test1"));

    let lane_id = LaneId::new(node1.mempool_ctx.validator_pubkey().clone());
    let (dp, dp_msg, votes) = disseminate! {
        txs: [register_tx],
        owner: node1.mempool_ctx,
        voters: [node2.mempool_ctx, node3.mempool_ctx]
    };

    let assert_nb_signatures = |node: &AutobahnTestCtx, n: usize| {
        assert_eq!(
            node.mempool_ctx.last_lane_entry(&lane_id).1,
            dp.clone().hashed()
        );
        assert_eq!(
            node.mempool_ctx
                .last_lane_entry(&lane_id)
                .0
                .0
                .signatures
                .len(),
            n
        );
    };

    assert_nb_signatures(&node2, 3);
    assert_nb_signatures(&node3, 3);

    assert_eq!(node4.mempool_ctx.current_hash(&lane_id), None);

    // Handle votes before data proposal (simulate a data proposal still being processed, not recorded yet)

    node4
        .mempool_ctx
        .handle_msg(&votes[0], "Vote handling")
        .await;
    node4
        .mempool_ctx
        .handle_msg(&votes[1], "Vote handling 2")
        .await;
    node4.mempool_ctx.handle_msg(&dp_msg, "Data Proposal").await;

    node4.mempool_ctx.handle_processed_data_proposals().await;

    // 4 because the 3 signatures are the ones of the other nodes, so + the node 4 signature it makes 4 signatures
    assert_nb_signatures(&node4, 4);

    broadcast! {
        description: "Disseminated Tx Vote",
        from: node4.mempool_ctx, to: [node1.mempool_ctx, node2.mempool_ctx, node3.mempool_ctx],
        message_matches: MempoolNetMessage::DataVote(..)
    };
    node1
        .mempool_ctx
        .process_dissemination_events()
        .await
        .expect("process dissemination events");

    assert_nb_signatures(&node2, 4);
    assert_nb_signatures(&node3, 4);
    assert_nb_signatures(&node4, 4);
}

#[test_log::test(tokio::test)]
async fn consensus_missed_prepare_then_timeout() {
    // Scenario here - a node misses a prepare at round 2, then timeout at R3V0,
    // Upon receiving the Prepare for R3V1, it seems a QC for R2, which it can process,
    // thus catching up.
    let (mut node1, mut node2, mut node3, mut node4) = build_nodes!(4).await;

    // First data proposal

    let register_tx = make_register_contract_tx(ContractName::new("test1"));

    disseminate! {
        txs: [register_tx],
        owner: node1.mempool_ctx,
        voters: [node2.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx]
    };

    node1
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    // Normal round from Genesis
    simple_commit_round! {
        leader: node1.consensus_ctx,
        followers: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx]
    };

    let register_tx = make_register_contract_tx(ContractName::new("test1"));
    disseminate! {
        txs: [register_tx],
        owner: node2.mempool_ctx,
        voters: [node1.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx]
    };

    node2
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;

    // Node 3 misses the prepare
    simple_commit_round! {
        leader: node2.consensus_ctx,
        followers: [node1.consensus_ctx, node4.consensus_ctx]
    };

    ConsensusTestCtx::timeout(&mut [
        &mut node1.consensus_ctx,
        &mut node2.consensus_ctx,
        &mut node4.consensus_ctx,
    ])
    .await;

    broadcast! {
        description: "Follower - Timeout",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };

    // node 3 should join the mutiny
    broadcast! {
        description: "Follower - Timeout",
        from: node4.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };

    node1
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 1")
        .await;
    node2
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 2")
        .await;

    // Node 4 is the next leader slot 3 view 1, has built the tc by joining the mutiny

    node4
        .start_round_with_cut_from_mempool(TimestampMs(4000))
        .await;

    broadcast! {
        description: "Leader Prepare with TC ticket",
        from: node4.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    // Node 3 can't vote yet
    // Node 3 needs to sync

    let sync_request = node3
        .consensus_ctx
        .assert_send(&node4.consensus_ctx.pubkey(), "Sync Request")
        .await;

    node4
        .consensus_ctx
        .handle_msg(&sync_request, "Handling Sync request")
        .await;

    let sync_reply = node4
        .consensus_ctx
        .assert_send(&node3.consensus_ctx.validator_pubkey(), "SyncReply")
        .await;

    node3
        .consensus_ctx
        .handle_msg(&sync_reply, "Handling Sync reply")
        .await;

    send! {
        description: "Voting after sync",
        from: [node3.consensus_ctx], to: node4.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(..)
    };

    // Others vote too
    send! {
        description: "Voting",
        from: [node1.consensus_ctx, node2.consensus_ctx], to: node4.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(..)
    };

    broadcast! {
        description: "Confirm",
        from: node4.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx], to: node4.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    broadcast! {
        description: "Commit",
        from: node4.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };

    // Weirdly, next leader is node 4 too on slot: 4 view: 0

    node4
        .start_round_with_cut_from_mempool(TimestampMs(5000))
        .await;

    // Making sure Consensus goes on

    simple_commit_round! {
        leader: node4.consensus_ctx,
        followers: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx]
    };
}

#[test_log::test(tokio::test)]
async fn mempool_fail_to_vote_on_fork() {
    let (mut node1, mut node2, mut node3, mut node4) = build_nodes!(4).await;

    // Let's create data proposals that will be synchronized with 2 nodes, but not the 3rd.

    // First data proposal

    let register_tx = make_register_contract_tx(ContractName::new("test1"));
    let register_tx_2 = make_register_contract_tx(ContractName::new("test2"));

    let dp1 = node1
        .mempool_ctx
        .create_data_proposal(None, &[register_tx, register_tx_2]);
    node1
        .mempool_ctx
        .process_new_data_proposal(dp1.clone())
        .unwrap();
    node1.mempool_ctx.timer_tick().await.unwrap();

    let message = broadcast! {
        description: "Disseminate Tx",
        from: node1.mempool_ctx, to: [node2.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx],
        message_matches: MempoolNetMessage::DataProposal(..)
    };
    let dp1_check = match &message.msg {
        MempoolNetMessage::DataProposal(_, _, data, _) => data.clone(),
        _ => panic!("Expected DataProposal message"),
    };

    assert_eq!(dp1, dp1_check);

    join_all(
        [
            &mut node2.mempool_ctx,
            &mut node3.mempool_ctx,
            &mut node4.mempool_ctx,
        ]
        .iter_mut()
        .map(|ctx| ctx.handle_processed_data_proposals()),
    )
    .await;

    broadcast! {
        description: "Disseminated Tx Vote",
        from: node2.mempool_ctx, to: [node1.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx],
        message_matches: MempoolNetMessage::DataVote(..)
    };
    broadcast! {
        description: "Disseminated Tx Vote",
        from: node3.mempool_ctx, to: [node1.mempool_ctx, node2.mempool_ctx, node4.mempool_ctx],
        message_matches: MempoolNetMessage::DataVote(..)
    };
    broadcast! {
        description: "Disseminated Tx Vote",
        from: node4.mempool_ctx, to: [node1.mempool_ctx, node2.mempool_ctx, node3.mempool_ctx],
        message_matches: MempoolNetMessage::DataVote(..)
    };

    node1
        .mempool_ctx
        .process_dissemination_events()
        .await
        .expect("process dissemination events");

    // Second data proposal

    let register_tx = make_register_contract_tx(ContractName::new("test3"));
    let register_tx_2 = make_register_contract_tx(ContractName::new("test4"));

    let dp2 = node1
        .mempool_ctx
        .create_data_proposal(Some(dp1.hashed()), &[register_tx, register_tx_2]);
    node1
        .mempool_ctx
        .process_new_data_proposal(dp2.clone())
        .unwrap();
    node1.mempool_ctx.timer_tick().await.unwrap();

    broadcast! {
        description: "Disseminate Tx",
        from: node1.mempool_ctx, to: [node2.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx],
        message_matches: MempoolNetMessage::DataProposal(..)
    };

    join_all(
        [
            &mut node2.mempool_ctx,
            &mut node3.mempool_ctx,
            &mut node4.mempool_ctx,
        ]
        .iter_mut()
        .map(|ctx| ctx.handle_processed_data_proposals()),
    )
    .await;

    broadcast! {
        description: "Disseminated Tx Vote",
        from: node2.mempool_ctx, to: [node1.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx],
        message_matches: MempoolNetMessage::DataVote(..)
    };
    broadcast! {
        description: "Disseminated Tx Vote",
        from: node3.mempool_ctx, to: [node1.mempool_ctx, node2.mempool_ctx, node4.mempool_ctx],
        message_matches: MempoolNetMessage::DataVote(..)
    };
    broadcast! {
        description: "Disseminated Tx Vote",
        from: node4.mempool_ctx, to: [node1.mempool_ctx, node2.mempool_ctx, node3.mempool_ctx],
        message_matches: MempoolNetMessage::DataVote(..)
    };

    // BASIC FORK
    // dp1 <- dp2
    //     <- dp3

    let dp_fork_3 = DataProposal::new(dp1.hashed(), vec![Transaction::default()]);
    let cumul_size = node1
        .mempool_ctx
        .current_size_of(&node1.mempool_ctx.own_lane())
        .unwrap_or_default()
        + dp_fork_3.estimate_size();

    let data_proposal_fork_3 = node1
        .mempool_ctx
        .create_net_message(MempoolNetMessage::DataProposal(
            node1.mempool_ctx.own_lane(),
            dp_fork_3.hashed(),
            dp_fork_3.clone(),
            node1
                .mempool_ctx
                .sign_data((dp_fork_3.hashed(), cumul_size))
                .unwrap(),
        ))
        .unwrap();

    node2
        .mempool_ctx
        .handle_msg(&data_proposal_fork_3, "Fork 3")
        .await;
    node3
        .mempool_ctx
        .handle_msg(&data_proposal_fork_3, "Fork 3")
        .await;
    node4
        .mempool_ctx
        .handle_msg(&data_proposal_fork_3, "Fork 3")
        .await;

    // Check fork has not been consumed

    assert_ne!(
        node2
            .mempool_ctx
            .last_lane_entry(&node1.mempool_ctx.own_lane())
            .1,
        dp_fork_3.hashed()
    );
    assert_ne!(
        node3
            .mempool_ctx
            .last_lane_entry(&node1.mempool_ctx.own_lane())
            .1,
        dp_fork_3.hashed()
    );
    assert_ne!(
        node4
            .mempool_ctx
            .last_lane_entry(&node1.mempool_ctx.own_lane())
            .1,
        dp_fork_3.hashed()
    );
}

#[test_log::test(tokio::test)]
async fn autobahn_rejoin_flow_in_consensus_with_tc() {
    // Ordered so that node1 is leader at slot 3
    let (mut node3, mut node2, mut node1, mut joining_node) = build_nodes!(4).await;

    // Let's setup the consensus so our joining node has some blocks to catch up.
    ConsensusTestCtx::setup_for_round(
        &mut [
            &mut node1.consensus_ctx,
            &mut node2.consensus_ctx,
            &mut node3.consensus_ctx,
        ],
        3,
        0,
    );
    joining_node.consensus_ctx.setup_for_rejoining(0);

    // Let's setup a NodeState on this bus
    let mut ns = NodeState::create("test");
    let ns_event_sender = get_sender::<NodeStateEvent>(&joining_node.shared_bus).await;
    let mut ns_event_receiver = get_receiver::<NodeStateEvent>(&joining_node.shared_bus).await;
    let mut commit_receiver = get_receiver::<ConsensusEvent>(&node1.shared_bus).await;

    // Prep blocks 0, 1, 2
    let mut blocks = vec![SignedBlock {
        data_proposals: vec![],
        certificate: AggregateSignature::default(),
        consensus_proposal: ConsensusProposal {
            slot: 0,
            ..ConsensusProposal::default()
        },
    }];
    for _ in 0..2 {
        blocks.push(SignedBlock {
            data_proposals: vec![],
            certificate: AggregateSignature::default(),
            consensus_proposal: ConsensusProposal {
                slot: blocks.len() as u64,
                parent_hash: blocks[blocks.len() - 1].hashed(),
                ..ConsensusProposal::default()
            },
        });
    }

    // Catchup up to the last block, but don't actually process the last block message yet.
    for signed_block in blocks.get(0..blocks.len() - 1).unwrap() {
        let node_state_block = ns.handle_signed_block(signed_block.clone()).unwrap();
        ns_event_sender
            .send(BusEnvelope::from_message(NodeStateEvent::NewBlock(
                node_state_block,
            )))
            .unwrap();
    }
    while let Ok(event) = ns_event_receiver.try_recv() {
        let event = event.into_message();
        info!("{:?}", event);
        joining_node
            .consensus_ctx
            .handle_node_state_event(event)
            .await
            .expect("should handle data event");
    }

    // At this point DA is caught up, node state is missing one block,
    // consensus is still on slot 0.

    info!("Starting rounds...");

    // Do one round - we won't catch up yet.
    node1
        .start_round_with_cut_from_mempool(TimestampMs(1000 * 3))
        .await;
    simple_commit_round! {
        leader: node1.consensus_ctx,
        followers: [node2.consensus_ctx, node3.consensus_ctx],
        joining: joining_node.consensus_ctx
    };

    // Now process block 2
    let signed_block = blocks.get(2).unwrap().clone();
    let node_state_block = ns.handle_signed_block(signed_block).unwrap();
    ns_event_sender
        .send(BusEnvelope::from_message(NodeStateEvent::NewBlock(
            node_state_block,
        )))
        .unwrap();
    while let Ok(event) = ns_event_receiver.try_recv() {
        let event = event.into_message();
        info!("{:?}", event);
        joining_node
            .consensus_ctx
            .handle_node_state_event(event)
            .await
            .expect("should handle data event");
    }

    // We still aren't caught up
    assert!(joining_node.consensus_ctx.is_joining());

    // Now we are leader, but we are not caught up, so everyone time else times out.
    ConsensusTestCtx::timeout(&mut [
        &mut node1.consensus_ctx,
        &mut node2.consensus_ctx,
        &mut node3.consensus_ctx,
    ])
    .await;

    broadcast! {
        description: "Follower - Timeout",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx], // joining_node skipped to avoid spurious errors
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx], // joining_node skipped to avoid spurious errors
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node3.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx], // joining_node skipped to avoid spurious errors
        message_matches: ConsensusNetMessage::Timeout(..)
    };

    broadcast! {
        description: "Follower - Timeout Certificate",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx], // joining_node skipped to avoid spurious errors
        message_matches: ConsensusNetMessage::TimeoutCertificate(..)
    };

    info!("Followers timeout");

    // At this point, slot 4, view 1 is started by node 3.
    node3
        .start_round_with_cut_from_mempool(TimestampMs(4100))
        .await;

    simple_commit_round! {
        leader: node3.consensus_ctx,
        followers: [node1.consensus_ctx, node2.consensus_ctx],
        joining: joining_node.consensus_ctx
    };

    // We still aren't caught up
    assert!(joining_node.consensus_ctx.is_joining());

    // Catch up the two blocks
    for _ in 0..2 {
        while let Ok(event) = commit_receiver.try_recv() {
            let ConsensusEvent::CommitConsensusProposal(ccp) = event.into_message();
            if ccp.consensus_proposal.slot > 2 {
                let signed_block = SignedBlock {
                    data_proposals: vec![],
                    certificate: ccp.certificate,
                    consensus_proposal: ccp.consensus_proposal,
                };
                let node_state_block = ns.handle_signed_block(signed_block.clone()).unwrap();
                ns_event_sender
                    .send(BusEnvelope::from_message(NodeStateEvent::NewBlock(
                        node_state_block,
                    )))
                    .unwrap();

                blocks.push(signed_block);
            }
        }
        while let Ok(event) = ns_event_receiver.try_recv() {
            let event = event.into_message();
            joining_node
                .consensus_ctx
                .handle_node_state_event(event)
                .await
                .expect("should handle data event");
        }
    }

    // Now node3 (again, quirk of current leader selection) starts slot 5 view 0
    node3
        .start_round_with_cut_from_mempool(TimestampMs(5000))
        .await;

    simple_commit_round! {
        leader: node3.consensus_ctx,
        followers: [node1.consensus_ctx, node2.consensus_ctx],
        joining: joining_node.consensus_ctx
    };

    // We are caught up
    assert!(!joining_node.consensus_ctx.is_joining());
}

#[test_log::test(tokio::test)]
// Test an edge case where all nodes are stopped and must restart from a certain height.
// Case A is via timeout certificates.
async fn autobahn_all_restart_flow_a() {
    let (mut node0, mut node1, mut node2, mut node3) = build_nodes!(4).await;

    ConsensusTestCtx::setup_for_round(
        &mut [
            &mut node0.consensus_ctx,
            &mut node1.consensus_ctx,
            &mut node2.consensus_ctx,
            &mut node3.consensus_ctx,
        ],
        3,
        0,
    );

    // Mark all nodes as joining
    for node in [&mut node0, &mut node1, &mut node2, &mut node3] {
        node.consensus_ctx.setup_for_rejoining(3);
    }

    // Nodes 0/1/2 time out
    // Node 3 will receive the TC directly to test both paths
    // (node 3 also happens to be leader)
    ConsensusTestCtx::timeout(&mut [
        &mut node0.consensus_ctx,
        &mut node1.consensus_ctx,
        &mut node2.consensus_ctx,
    ])
    .await;
    broadcast! {
        description: "Follower - Timeout",
        from: node0.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node1.consensus_ctx, to: [node0.consensus_ctx, node2.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node2.consensus_ctx, to: [node0.consensus_ctx, node1.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout Certificate",
        from: node0.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::TimeoutCertificate(..)
    };

    // Now node3 starts slot 3 view 1
    node3
        .start_round_with_cut_from_mempool(TimestampMs(4100))
        .await;
    simple_commit_round! {
        leader: node3.consensus_ctx,
        followers: [node0.consensus_ctx, node1.consensus_ctx, node2.consensus_ctx]
    };
}

#[test_log::test(tokio::test)]
// Test an edge case where all nodes are stopped and must restart from a certain height.
// Case B is directly via the timeout flow.
async fn autobahn_all_restart_flow_b() {
    let (mut node0, mut node1, mut node2, mut node3) = build_nodes!(4).await;

    ConsensusTestCtx::setup_for_round(
        &mut [
            &mut node0.consensus_ctx,
            &mut node1.consensus_ctx,
            &mut node2.consensus_ctx,
            &mut node3.consensus_ctx,
        ],
        3,
        0,
    );

    // Mark all nodes as joining
    for node in [&mut node0, &mut node1, &mut node2, &mut node3] {
        node.consensus_ctx.setup_for_rejoining(3);
    }

    // Nodes 0/1/2/3 time out
    ConsensusTestCtx::timeout(&mut [
        &mut node0.consensus_ctx,
        &mut node1.consensus_ctx,
        &mut node2.consensus_ctx,
        &mut node3.consensus_ctx,
    ])
    .await;
    broadcast! {
        description: "Follower - Timeout",
        from: node0.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node1.consensus_ctx, to: [node0.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node2.consensus_ctx, to: [node0.consensus_ctx, node1.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node3.consensus_ctx, to: [node0.consensus_ctx, node1.consensus_ctx, node2.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };

    // Now node3 starts slot 3 view 1
    node3
        .start_round_with_cut_from_mempool(TimestampMs(4100))
        .await;
    simple_commit_round! {
        leader: node3.consensus_ctx,
        followers: [node0.consensus_ctx, node1.consensus_ctx, node2.consensus_ctx]
    };
}

#[test_log::test(tokio::test)]
async fn autobahn_rejoin_flow_outside_consensus() {
    let (mut node1, mut node2) = build_nodes!(2).await;

    // Let's setup the consensus so our joining node has some blocks to catch up.
    ConsensusTestCtx::setup_for_round(
        &mut [&mut node1.consensus_ctx, &mut node2.consensus_ctx],
        3,
        0,
    );

    let crypto = BlstCrypto::new("node-3").unwrap();
    let mut joining_node = AutobahnTestCtx::new("node-3", crypto).await;
    joining_node
        .consensus_ctx
        .setup_for_joining(&[&node1.consensus_ctx, &node2.consensus_ctx]);

    // Let's setup a NodeState on this bus
    let mut ns = NodeState::create("test");

    let mut blocks = vec![SignedBlock {
        data_proposals: vec![],
        certificate: AggregateSignature::default(),
        consensus_proposal: ConsensusProposal {
            slot: 0,
            ..ConsensusProposal::default()
        },
    }];

    for _ in 0..2 {
        blocks.push(SignedBlock {
            data_proposals: vec![],
            certificate: AggregateSignature::default(),
            consensus_proposal: ConsensusProposal {
                slot: blocks.len() as u64,
                parent_hash: blocks[blocks.len() - 1].hashed(),
                ..ConsensusProposal::default()
            },
        });
    }

    let ns_event_sender = get_sender::<NodeStateEvent>(&joining_node.shared_bus).await;
    let mut ns_event_receiver = get_receiver::<NodeStateEvent>(&joining_node.shared_bus).await;
    let mut commit_receiver = get_receiver::<ConsensusEvent>(&node1.shared_bus).await;

    // Catchup up to the last block, but don't actually process the last block message yet.
    for signed_block in blocks.get(0..blocks.len() - 1).unwrap() {
        let node_state_block = ns.handle_signed_block(signed_block.clone()).unwrap();
        ns_event_sender
            .send(BusEnvelope::from_message(NodeStateEvent::NewBlock(
                node_state_block,
            )))
            .unwrap();
    }
    while let Ok(event) = ns_event_receiver.try_recv() {
        let event = event.into_message();
        info!("{:?}", event);
        joining_node
            .consensus_ctx
            .handle_node_state_event(event)
            .await
            .expect("should handle data event");
    }

    // Do a few rounds of consensus-with-lag and note that we don't actually catch up.
    // (this is expected because DA stopped receiving new blocks, as it did indeed catch up)
    for i in 1..4 {
        node1
            .start_round_with_cut_from_mempool(TimestampMs(1000 * i))
            .await;

        simple_commit_round! {
            leader: node1.consensus_ctx,
            followers: [node2.consensus_ctx],
            joining: joining_node.consensus_ctx
        };

        // Swap so we handle leader changes correctly
        std::mem::swap(&mut node1, &mut node2);
    }

    // Now process block 2
    let signed_block = blocks.get(2).unwrap().clone();
    let node_state_block = ns.handle_signed_block(signed_block).unwrap();
    ns_event_sender
        .send(BusEnvelope::from_message(NodeStateEvent::NewBlock(
            node_state_block,
        )))
        .unwrap();

    while let Ok(event) = ns_event_receiver.try_recv() {
        let event = event.into_message();
        info!("{:?}", event);
        joining_node
            .consensus_ctx
            .handle_node_state_event(event)
            .await
            .expect("should handle data event");
    }

    // Process round
    node1
        .start_round_with_cut_from_mempool(TimestampMs(5000))
        .await;

    simple_commit_round! {
        leader: node1.consensus_ctx,
        followers: [node2.consensus_ctx],
        joining: joining_node.consensus_ctx
    };

    std::mem::swap(&mut node1, &mut node2);

    // We still aren't caught up
    assert!(joining_node.consensus_ctx.is_joining());

    // Catch up
    for _ in 0..4 {
        while let Ok(event) = commit_receiver.try_recv() {
            let ConsensusEvent::CommitConsensusProposal(ccp) = event.into_message();
            if ccp.consensus_proposal.slot > 2 {
                let signed_block = SignedBlock {
                    data_proposals: vec![],
                    certificate: ccp.certificate,
                    consensus_proposal: ccp.consensus_proposal,
                };
                let node_state_block = ns.handle_signed_block(signed_block.clone()).unwrap();
                ns_event_sender
                    .send(BusEnvelope::from_message(NodeStateEvent::NewBlock(
                        node_state_block,
                    )))
                    .unwrap();

                blocks.push(signed_block);
            }
        }
        while let Ok(event) = ns_event_receiver.try_recv() {
            let event = event.into_message();
            joining_node
                .consensus_ctx
                .handle_node_state_event(event)
                .await
                .expect("should handle data event");
        }
    }

    // Process round
    node1
        .start_round_with_cut_from_mempool(TimestampMs(6000))
        .await;

    simple_commit_round! {
        leader: node1.consensus_ctx,
        followers: [node2.consensus_ctx],
        joining: joining_node.consensus_ctx
    };

    // We are caught up
    assert!(!joining_node.consensus_ctx.is_joining());
}

#[test_log::test(tokio::test)]
async fn protocol_fees() {
    let (mut node1, mut node2, mut node3, mut node4) = build_nodes!(4).await;

    // First data proposal

    let register_tx = make_register_contract_tx(ContractName::new("test1"));
    let register_tx_2 = make_register_contract_tx(ContractName::new("test2"));

    let (dp, _, _) = disseminate! {
        txs: [register_tx, register_tx_2],
        owner: node1.mempool_ctx,
        voters: [node2.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx]
    };

    let dp_size_1 = LaneBytesSize(dp.estimate_size() as u64);
    assert_eq!(node1.mempool_ctx.current_size(), Some(dp_size_1));
    assert_eq!(
        node1
            .mempool_ctx
            .current_size_of(&node2.mempool_ctx.own_lane()),
        None
    );

    assert_eq!(node2.mempool_ctx.current_size(), None);
    assert_eq!(
        node2
            .mempool_ctx
            .current_size_of(&node1.mempool_ctx.own_lane()),
        Some(dp_size_1)
    );
    assert_eq!(
        node3
            .mempool_ctx
            .current_size_of(&node1.mempool_ctx.own_lane()),
        Some(dp_size_1)
    );
    assert_eq!(
        node4
            .mempool_ctx
            .current_size_of(&node1.mempool_ctx.own_lane()),
        Some(dp_size_1)
    );

    // Second data proposal

    let register_tx = make_register_contract_tx(ContractName::new("test3"));
    let register_tx_2 = make_register_contract_tx(ContractName::new("test4"));
    let register_tx_3 = make_register_contract_tx(ContractName::new("test5"));

    let (dp, _, _) = disseminate! {
        txs: [register_tx, register_tx_2, register_tx_3],
        owner: node2.mempool_ctx,
        voters: [node1.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx]
    };

    let dp_size_2 = LaneBytesSize(dp.estimate_size() as u64);
    assert_eq!(node2.mempool_ctx.current_size(), Some(dp_size_2));
    assert_eq!(
        node2
            .mempool_ctx
            .current_size_of(&node1.mempool_ctx.own_lane()),
        Some(dp_size_1)
    );

    assert_eq!(node1.mempool_ctx.current_size(), Some(dp_size_1));
    assert_eq!(
        node1
            .mempool_ctx
            .current_size_of(&node2.mempool_ctx.own_lane()),
        Some(dp_size_2)
    );

    assert_eq!(
        node3
            .mempool_ctx
            .current_size_of(&node1.mempool_ctx.own_lane()),
        Some(dp_size_1)
    );
    assert_eq!(
        node3
            .mempool_ctx
            .current_size_of(&node2.mempool_ctx.own_lane()),
        Some(dp_size_2)
    );

    // Let's do a consensus round

    node1
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(cp, ..) => {
            assert_eq!(cp.staking_actions.len(), 2);
            assert_eq!(
                cp.staking_actions[0],
                ConsensusStakingAction::PayFeesForDaDi {
                    lane_id: node1.mempool_ctx.own_lane(),
                    cumul_size: dp_size_1
                }
            );
            assert_eq!(
                cp.staking_actions[1],
                ConsensusStakingAction::PayFeesForDaDi {
                    lane_id: node2.mempool_ctx.own_lane(),
                    cumul_size: dp_size_2
                }
            );
        }
    };
}

/// P = Proposal
/// Cf = Confirm
/// C = Commit
///
/// Normal case: P1 -> Cf1 -> C1 -> P2 -> Cf2 -> C2
/// Test case: P1 -> C1 -> P2 -> C2
///
/// Confirm messages can be ignored if not received
#[test_log::test(tokio::test)]
async fn autobahn_missed_a_confirm_message() {
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

    node1
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    // Slot from node-1 not yet sent to node 4
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

    // Node 4 doesn't receive confirm
    broadcast! {
        description: "Confirm",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node2.consensus_ctx, node3.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    // But Node 4 receive commit
    broadcast! {
        description: "Commit",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };

    // Slot 6 starts with new leader, sending to all
    node2
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node1.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    broadcast! {
        description: "Confirm",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node1.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    broadcast! {
        description: "Commit",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };
}

/// P = Proposal
/// Cf = Confirm
/// C = Commit
///
/// Normal case: P1 -> Cf1 -> C1 -> P2 -> Cf2 -> C2
/// Test case: P1 -> P2 -> Cf2 -> C2
///
/// Confirm and commit can be ignored if next porposal is received
#[test_log::test(tokio::test)]
async fn autobahn_missed_confirm_and_commit_messages() {
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

    node1
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    broadcast! {
        description: "Prepare - Slot from node-1 not yet sent to node 4",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    broadcast! {
        description: "Confirm - Node 4 doesn't receive confirm",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node2.consensus_ctx, node3.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    broadcast! {
        description: "Commit - Node 4 doesn't receive commit",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };

    // Slot 6 starts with new leader, sending to all
    node2
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote - Node 4 has fast-forwarded to the next proposal",
        from: [node1.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    broadcast! {
        description: "Confirm",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node1.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    broadcast! {
        description: "Commit",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };
}

#[test_log::test(tokio::test)]
async fn leader_receives_commit_for_next_slot() {
    let (mut node0, mut node1, mut node2, mut node3) = build_nodes!(4).await;

    ConsensusTestCtx::setup_for_round(
        &mut [
            &mut node0.consensus_ctx,
            &mut node1.consensus_ctx,
            &mut node2.consensus_ctx,
            &mut node3.consensus_ctx,
        ],
        5,
        0,
    );

    node0
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node0.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx], to: node0.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    broadcast! {
        description: "Confirm",
        from: node0.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx], to: node0.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    let commit_msg = broadcast! {
        description: "Commit",
        from: node0.consensus_ctx, to: [],
        message_matches: ConsensusNetMessage::Commit(..)
    };

    ConsensusTestCtx::timeout(&mut [
        &mut node1.consensus_ctx,
        &mut node2.consensus_ctx,
        &mut node3.consensus_ctx,
    ])
    .await;

    broadcast! {
        description: "Follower - Timeout",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node3.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };

    // Node1 is next leader (slot 5 + view 1 = 6 % 4 = 1)
    node1
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;

    broadcast! {
        description: "Leader - Prepare (view 1)",
        from: node1.consensus_ctx, to: [node0.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    node1
        .consensus_ctx
        .handle_msg(&commit_msg, "Leader receives commit for next slot")
        .await;

    assert_eq!(node1.consensus_ctx.slot(), 6);
    assert_eq!(node1.consensus_ctx.view(), 0);
    assert!(node1.consensus_ctx.current_proposal().is_none());
}

#[test_log::test(tokio::test)]
async fn autobahn_got_timed_out_during_sync() {
    // node1 is 2nd leader but got disconnected
    let (mut node0, mut node1, mut node2, mut node3) = build_nodes!(4).await;

    ConsensusTestCtx::setup_for_round(
        &mut [
            &mut node0.consensus_ctx,
            &mut node1.consensus_ctx,
            &mut node2.consensus_ctx,
            &mut node3.consensus_ctx,
        ],
        5,
        0,
    );

    // Slot 5 starts, all nodes receive the prepare
    node0
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node0.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx], to: node0.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    broadcast! {
        description: "Confirm - Node1 disconnected",
        from: node0.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node2.consensus_ctx, node3.consensus_ctx], to: node0.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    broadcast! {
        description: "Commit - Node1 still disconnected",
        from: node0.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };

    // Make node0 and node2 timeout, node3 will not timeout but follow mutiny
    // , because at f+1, mutiny join
    ConsensusTestCtx::timeout(&mut [&mut node0.consensus_ctx, &mut node2.consensus_ctx]).await;

    broadcast! {
        description: "Follower - Timeout",
        from: node0.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node2.consensus_ctx, to: [node0.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };

    // node 3 should join the mutiny
    broadcast! {
        description: "Follower - Timeout",
        from: node3.consensus_ctx, to: [node0.consensus_ctx, node2.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };

    node0
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 1")
        .await;
    // Node 2 is next leader, and does not emits a timeout certificate since it will broadcast the next Prepare with it
    node2
        .consensus_ctx
        .assert_no_broadcast("Timeout Certificate 3");
    node3
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 4")
        .await;

    // Slot 6 starts with new leader with node1 disconnected
    node2
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node2.consensus_ctx, to: [node0.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node0.consensus_ctx, node3.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    broadcast! {
        description: "Confirm",
        from: node2.consensus_ctx, to: [node0.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node0.consensus_ctx, node3.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    broadcast! {
        description: "Commit",
        from: node2.consensus_ctx, to: [node0.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };

    // Slot 5 starts but node1 is back online - leader is again node2
    node2
        .start_round_with_cut_from_mempool(TimestampMs(3000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node2.consensus_ctx, to: [node0.consensus_ctx, node1.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node0.consensus_ctx, node3.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    let confirm = node2.consensus_ctx.assert_broadcast("Confirm").await;

    send! {
        description: "SyncRequest - Node1 ask for missed proposal Slot 4",
        from: [node1.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::SyncRequest(_)
    };

    send! {
        description: "SyncReply - Node 2 replies with proposal Slot 4",
        from: [node2.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::SyncReply(_)
    };

    send! {
        description: "PrepareVote - Node1 votes on slot 7",
        from: [node1.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    node0
        .consensus_ctx
        .handle_msg(
            &confirm,
            "[handling broadcast message from: node2 at: node0] Confirm",
        )
        .await;
    node1
        .consensus_ctx
        .handle_msg(
            &confirm,
            "[handling broadcast message from: node2 at: node1] Confirm",
        )
        .await;
    node3
        .consensus_ctx
        .handle_msg(
            &confirm,
            "[handling broadcast message from: node2 at: node3] Confirm",
        )
        .await;

    send! {
        description: "ConfirmAck",
        from: [node0.consensus_ctx, node1.consensus_ctx, node3.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    broadcast! {
        description: "Commit",
        from: node2.consensus_ctx, to: [node0.consensus_ctx, node1.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };
}

#[test_log::test(tokio::test)]
async fn autobahn_timeout_split_views_no_tc() {
    let (mut node0, mut node1, mut node2, mut node3) = build_nodes!(4).await;

    ConsensusTestCtx::setup_for_round(
        &mut [
            &mut node0.consensus_ctx,
            &mut node1.consensus_ctx,
            &mut node2.consensus_ctx,
            &mut node3.consensus_ctx,
        ],
        5,
        0,
    );

    ConsensusTestCtx::timeout(&mut [
        &mut node0.consensus_ctx,
        &mut node1.consensus_ctx,
        &mut node2.consensus_ctx,
        &mut node3.consensus_ctx,
    ])
    .await;

    broadcast! {
        description: "Timeout v0 -> node2/node3",
        from: node0.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Timeout v0 -> node2/node3",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Timeout v0 -> dropped",
        from: node2.consensus_ctx, to: [],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Timeout v0 -> dropped",
        from: node3.consensus_ctx, to: [],
        message_matches: ConsensusNetMessage::Timeout(..)
    };

    broadcast! {
        description: "Timeout Certificate node2",
        from: node2.consensus_ctx, to: [],
        message_matches: ConsensusNetMessage::TimeoutCertificate(..)
    };
    broadcast! {
        description: "Timeout Certificate node3",
        from: node3.consensus_ctx, to: [],
        message_matches: ConsensusNetMessage::TimeoutCertificate(..)
    };

    node0.consensus_ctx.assert_no_broadcast("No TC from node0");
    node1.consensus_ctx.assert_no_broadcast("No TC from node1");

    ConsensusTestCtx::timeout(&mut [&mut node0.consensus_ctx]).await;

    broadcast! {
        description: "Timeout v0 to node2",
        from: node0.consensus_ctx, to: [node2.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout((signed_slot_view, _)) => {
            assert_eq!(signed_slot_view.msg.1, 0);
        }
    };

    send! {
        description: "TimeoutCertificate reply to old view",
        from: [
            node2.consensus_ctx; ConsensusNetMessage::TimeoutCertificate(_, _, slot, view) => {
                assert_eq!(*slot, 5);
                assert_eq!(*view, 0);
            }
        ], to: node0.consensus_ctx
    };
}

#[test_log::test(tokio::test)]
async fn autobahn_commit_different_views_for_f() {
    let (mut node0, mut node1, mut node2, mut node3) = build_nodes!(4).await;

    ConsensusTestCtx::setup_for_round(
        &mut [
            &mut node0.consensus_ctx,
            &mut node1.consensus_ctx,
            &mut node2.consensus_ctx,
            &mut node3.consensus_ctx,
        ],
        5,
        0,
    );

    // Slot 5 starts, all nodes receive the prepare
    node0
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node0.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx], to: node0.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    broadcast! {
        description: "Confirm",
        from: node0.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx], to: node0.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    // At this point, node0 has committed S5V0. It now disconnects.
    // (process the broadcast silently)
    broadcast! {
        description: "Commit",
        from: node0.consensus_ctx, to: [],
        message_matches: ConsensusNetMessage::Commit(..)
    };

    // Nodes 1, 2, 3 ultimately timeout.
    ConsensusTestCtx::timeout(&mut [
        &mut node1.consensus_ctx,
        &mut node2.consensus_ctx,
        &mut node3.consensus_ctx,
    ])
    .await;

    // Node 0 is still out.
    broadcast! {
        description: "Follower - Timeout",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node3.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };

    // Node 1 is next leader, and does not emits a timeout certificate since it will broadcast the next Prepare with it
    node1
        .consensus_ctx
        .assert_no_broadcast("Timeout Certificate 1");
    node2
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 2")
        .await;
    node3
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 3")
        .await;

    node1
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;

    simple_commit_round! {
        leader: node1.consensus_ctx,
        followers: [node2.consensus_ctx, node3.consensus_ctx]
    };

    // Node 1 is the leader of the next slot as well
    node1
        .start_round_with_last_seen_cut(TimestampMs(2000))
        .await;

    // At this point node 0 silently reconnects. However, since it has already committed slot 5,
    // it will emit a warn on receiving a different CQC than the one it buffered.
    // This still proceeds fine as we 'cheat' for now.
    simple_commit_round! {
        leader: node1.consensus_ctx,
        followers: [node0.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx]
    };
}

#[test_log::test(tokio::test)]
async fn autobahn_commit_different_views_for_fplusone() {
    let (mut node0, mut node1, mut node2, mut node3) = build_nodes!(4).await;

    ConsensusTestCtx::setup_for_round(
        &mut [
            &mut node0.consensus_ctx,
            &mut node1.consensus_ctx,
            &mut node2.consensus_ctx,
            &mut node3.consensus_ctx,
        ],
        5,
        0,
    );

    // Slot 5 starts, all nodes receive the prepare
    node0
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node0.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx], to: node0.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    broadcast! {
        description: "Confirm",
        from: node0.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx], to: node0.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    // Simulate a weird case - node0 has committed the new proposal, and node3 has received the Commit message.
    // We know have two nodes that have committed, and two that haven't and timeout.
    broadcast! {
        description: "Node3 commits",
        from: node0.consensus_ctx, to: [node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };

    // Nodes 1, 2 ultimately timeout, broadcasting to all now reconnected.
    ConsensusTestCtx::timeout(&mut [&mut node1.consensus_ctx, &mut node2.consensus_ctx]).await;
    broadcast! {
        description: "Follower - Timeout",
        from: node1.consensus_ctx, to: [node0.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node2.consensus_ctx, to: [node0.consensus_ctx, node1.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };

    // Nobody makes a TC as the nodes that have already committed won't answer.
    node1
        .consensus_ctx
        .assert_no_broadcast("Timeout Certificate 1");
    node2
        .consensus_ctx
        .assert_no_broadcast("Timeout Certificate 2");

    // At this point we are essentially stuck.
    // TODO: fix this by sending commit messages to the nodes that are timing out so they can unlock themselves.
}

#[test_log::test(tokio::test)]
async fn autobahn_commit_byzantine_across_views_attempts() {
    let (mut node0, mut node1, mut node2, mut node3) = build_nodes!(4).await;

    ConsensusTestCtx::setup_for_round(
        &mut [
            &mut node0.consensus_ctx,
            &mut node1.consensus_ctx,
            &mut node2.consensus_ctx,
            &mut node3.consensus_ctx,
        ],
        5,
        0,
    );

    // Goal of the test: at slot 5 view 0, we have nodes voting on a prepare. A commit could in theory be created if someone side-channels the confirmacks
    // so we must ensure that we cannot commit another value. Attempt to do so and notice failures.
    // Node 1 fails to receive the initial proposal and proposes a different one.
    node0
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    let initial_cp;

    broadcast! {
        description: "Prepare",
        from: node0.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(cp, ..) => {
            initial_cp = cp.clone();
        }
    };

    send! {
        description: "PrepareVote",
        from: [node2.consensus_ctx, node3.consensus_ctx], to: node0.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    broadcast! {
        description: "Confirm",
        from: node0.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    // Check that they sent their vote
    node2
        .consensus_ctx
        .assert_send(&node0.consensus_ctx.validator_pubkey(), "confirmack")
        .await;
    node3
        .consensus_ctx
        .assert_send(&node0.consensus_ctx.validator_pubkey(), "confirmack")
        .await;

    // Nodes 0, 1, 2, 3 ultimately timeout.
    ConsensusTestCtx::timeout(&mut [
        &mut node0.consensus_ctx,
        &mut node1.consensus_ctx,
        &mut node2.consensus_ctx,
        &mut node3.consensus_ctx,
    ])
    .await;

    broadcast! {
        description: "Follower - Timeout",
        from: node0.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node1.consensus_ctx, to: [node0.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node2.consensus_ctx, to: [node0.consensus_ctx, node1.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout((_, TimeoutKind::PrepareQC(..)))
    };
    // node 3 won't even timeout, it will process the TC directly.

    // Node 1 is next leader, and does not emits a timeout certificate since it will broadcast the next Prepare with it
    node0
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 0")
        .await;
    node1
        .consensus_ctx
        .assert_no_broadcast("Timeout Certificate 1");
    node2
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 2")
        .await;
    node3
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 3")
        .await;

    // Change the proposal.
    let dp = node1.mempool_ctx.create_data_proposal(None, &[]);
    node1
        .mempool_ctx
        .process_new_data_proposal(dp.clone())
        .unwrap();
    node1.mempool_ctx.timer_tick().await.unwrap();

    node1
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;

    // Check that the node is reproposing the same.
    broadcast! {
        description: "Prepare",
        from: node1.consensus_ctx, to: [node0.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(cp, Ticket::TimeoutQC(.., TCKind::PrepareQC((_, tcp))), _) => {
            assert_eq!(cp, &initial_cp);
            assert_eq!(tcp, &initial_cp);
        }
    };
    // TODO: check that proposing something different will fail?
}

#[test_log::test(tokio::test)]
async fn autobahn_commit_prepare_qc_across_multiple_views() {
    let (mut node0, mut node1, mut node2, mut node3) = build_nodes!(4).await;

    ConsensusTestCtx::setup_for_round(
        &mut [
            &mut node0.consensus_ctx,
            &mut node1.consensus_ctx,
            &mut node2.consensus_ctx,
            &mut node3.consensus_ctx,
        ],
        5,
        0,
    );

    // Slot 5 starts, all nodes receive the prepare
    node0
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    let initial_cp;
    broadcast! {
        description: "Prepare",
        from: node0.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(cp, ..) => {
            initial_cp = cp.clone();
        }
    };

    send! {
        description: "PrepareVote",
        from: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx], to: node0.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    // Node0 gets the confirm message out but then crashes before sending commit
    broadcast! {
        description: "Confirm",
        from: node0.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx], to: node0.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    // First timeout - nodes 1,2,3 timeout and move to view 1
    ConsensusTestCtx::timeout(&mut [
        &mut node1.consensus_ctx,
        &mut node2.consensus_ctx,
        &mut node3.consensus_ctx,
    ])
    .await;

    broadcast! {
        description: "Follower - First Timeout",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - First Timeout",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - First Timeout",
        from: node3.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };

    // Node 1 is next leader (slot 5 + view 1 = 6 % 4 = 2), doesn't emit timeout certificate
    node1
        .consensus_ctx
        .assert_no_broadcast("Timeout Certificate 1");
    node2
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 2")
        .await;
    node3
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 3")
        .await;

    // Second timeout - move to view 2
    ConsensusTestCtx::timeout(&mut [
        &mut node1.consensus_ctx,
        &mut node2.consensus_ctx,
        &mut node3.consensus_ctx,
    ])
    .await;

    broadcast! {
        description: "Follower - Second Timeout",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Second Timeout",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Second Timeout",
        from: node3.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };

    // Node 2 is next leader (slot 5 + view 2 = 7 % 4 = 3), doesn't emit timeout certificate
    node1
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 1")
        .await;
    node2
        .consensus_ctx
        .assert_no_broadcast("Timeout Certificate 2");
    node3
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 3")
        .await;

    // Start next round with node2 as leader
    node2
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;

    // Check that node2 is reproposing the same CP from view 0
    broadcast! {
        description: "Prepare",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(cp, Ticket::TimeoutQC(.., TCKind::PrepareQC((_, tcp))), _) => {
            assert_eq!(cp, &initial_cp, "Leader must propose the same CP from view 0 even after multiple view changes");
            assert_eq!(tcp, &initial_cp, "TimeoutQC must reference the same CP from view 0");
        }
    };
}

#[test_log::test(tokio::test)]
async fn follower_commits_cut_then_mempool_sends_stale_lane() {
    // This test checks the following case:
    // - Node1 gets a 2 DPs on its lane
    // - Node2 hears of the first DP only
    // - Node1, as leader, commits the 2 dps
    // - Node2 gets a DP on its lane.
    // - Node2, as leader, commits the new DP and re-uses the 2nd DP from node1 that it doesn't know
    // (this last bit is where the code used to fail, as we would take our local DP1).
    let (mut node1, mut node2) = build_nodes!(2).await;

    // Node1 is leader, node2 is follower
    let register_tx = make_register_contract_tx(ContractName::new("test1"));
    let dp = node1.mempool_ctx.create_data_proposal(None, &[register_tx]);
    node1
        .mempool_ctx
        .process_new_data_proposal(dp.clone())
        .unwrap();
    node1.mempool_ctx.timer_tick().await.unwrap();

    let register_tx2 = make_register_contract_tx(ContractName::new("test2"));
    let dp1b = node1
        .mempool_ctx
        .create_data_proposal(Some(dp.hashed()), &[register_tx2]);
    node1
        .mempool_ctx
        .process_new_data_proposal(dp1b.clone())
        .unwrap();
    node1.mempool_ctx.timer_tick().await.unwrap();

    // Disseminate to node2
    broadcast! {
        description: "Disseminate Tx",
        from: node1.mempool_ctx, to: [node2.mempool_ctx],
        message_matches: MempoolNetMessage::DataProposal(..)
    };
    node2.mempool_ctx.handle_processed_data_proposals().await;
    broadcast! {
        description: "Disseminated Tx Vote",
        from: node2.mempool_ctx, to: [node1.mempool_ctx],
        message_matches: MempoolNetMessage::DataVote(..)
    };

    // Node1 starts round and commits cut
    node1
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    simple_commit_round! {
        leader: node1.consensus_ctx,
        followers: [node2.consensus_ctx]
    };

    let register_tx2 = make_register_contract_tx(ContractName::new("test2"));
    let dp2 = node2
        .mempool_ctx
        .create_data_proposal(None, &[register_tx2]);
    node2
        .mempool_ctx
        .process_new_data_proposal(dp2.clone())
        .unwrap();
    node2.mempool_ctx.timer_tick().await.unwrap();

    node2
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;

    let (cp, _, _) = simple_commit_round! {
        leader: node2.consensus_ctx,
        followers: [node1.consensus_ctx]
    };

    assert_eq!(
        CutDisplay(&cp.cut).to_string(),
        format!(
            "{}:{}({} B), {}:{}({} B),",
            LaneId::new(node1.consensus_ctx.validator_pubkey()),
            dp1b.hashed(),
            dp.estimate_size() + dp1b.estimate_size(),
            LaneId::new(node2.consensus_ctx.validator_pubkey()),
            dp2.hashed(),
            dp2.estimate_size(),
        )
    );
}

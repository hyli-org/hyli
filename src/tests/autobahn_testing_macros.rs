macro_rules! build_tuple {
    ($nodes:expr, 1) => {
        ($nodes)
    };
    ($nodes:expr, 2) => {
        ($nodes, $nodes)
    };
    ($nodes:expr, 3) => {
        ($nodes, $nodes, $nodes)
    };
    ($nodes:expr, 4) => {
        ($nodes, $nodes, $nodes, $nodes)
    };
    ($nodes:expr, 5) => {
        ($nodes, $nodes, $nodes, $nodes, $nodes)
    };
    ($nodes:expr, 6) => {
        ($nodes, $nodes, $nodes, $nodes, $nodes, $nodes)
    };
    ($nodes:expr, 7) => {
        ($nodes, $nodes, $nodes, $nodes, $nodes, $nodes, $nodes)
    };
    ($nodes:expr, $count:expr) => {
        panic!("More than {} nodes isn't supported", $count)
    };
}

macro_rules! broadcast {
    (description: $description:literal, from: $sender:expr, to: [$($node:expr),*]$(, message_matches: $pattern:pat $(=> $asserts:block)? )?) => {
        {
            // Construct the broadcast message with sender information
            let message = $sender.assert_broadcast(format!("[broadcast from: {}] {}", stringify!($sender), $description).as_str()).await;

            $({
                let msg_variant_name: &'static str = message.msg.clone().into();
                if let $pattern = &message.msg {
                    $($asserts)?
                } else {
                    panic!("[broadcast from: {}] {}: Message {} did not match {}", stringify!($sender), $description, msg_variant_name, stringify!($pattern));
                }
            })?

            // Distribute the message to each specified node
            $(
                $node.handle_msg(&message, (format!("[handling broadcast message from: {} at: {}] {}", stringify!($sender), stringify!($node), $description).as_str())).await;
            )*

            message
        }
    };
}

macro_rules! send {
    (
        description: $description:literal,
        from: [$($node:expr),+],
        to: $to:expr,
        message_matches: $pattern:pat
    ) => {
        // Distribute the message to the target node from all specified nodes
        ($({
            let answer = loop {
                let candidate = $node
                    .assert_send(
                        &$to.validator_pubkey(),
                        format!(
                            "[send from: {} to: {}] {}",
                            stringify!($node),
                            stringify!($to),
                            $description
                        )
                        .as_str(),
                    )
                    .await;

                if let $pattern = &candidate.msg {
                    break candidate;
                } else {
                    let msg_variant_name: &'static str = candidate.msg.clone().into();
                    info!(
                        "[send from: {}] {}: skipping {} while waiting for {}",
                        stringify!($node),
                        $description,
                        msg_variant_name,
                        stringify!($pattern)
                    );
                }
            };

            // Handle the message
            $to.handle_msg(
                &answer,
                format!("[handling sent message from: {} to: {}] {}", stringify!($node), stringify!($to), $description).as_str()
            ).await;
            answer
        },)+)
    };

    (
        description: $description:literal,
        from: [$($node:expr; $pattern:pat $(=> $asserts:block)?),+],
        to: $to:expr
    ) => {
        // Distribute the message to the target node from all specified nodes
        ($({
            let answer = loop {
                let candidate = $node
                    .assert_send(
                        &$to.validator_pubkey(),
                        format!(
                            "[send from: {} to: {}] {}",
                            stringify!($node),
                            stringify!($to),
                            $description
                        )
                        .as_str(),
                    )
                    .await;

                if let $pattern = &candidate.msg {
                    $($asserts)?
                    break candidate;
                } else {
                    let msg_variant_name: &'static str = candidate.msg.clone().into();
                    info!(
                        "[send from: {}] {}: skipping {} while waiting for {}",
                        stringify!($node),
                        $description,
                        msg_variant_name,
                        stringify!($pattern)
                    );
                }
            };

            // Handle the message
            $to.handle_msg(
                &answer,
                format!("[handling sent message from: {} to: {}] {}", stringify!($node), stringify!($to), $description).as_str()
            ).await;
        },)+)
    };
}

macro_rules! simple_commit_round {
    (leader: $leader:expr, followers: [$($follower:expr),+]$(, joining: $joining:expr)?) => {{
        let round_consensus_proposal;
        let round_ticket;
        let view: u64;

        broadcast! {
            description: "Leader - Prepare",
            from: $leader, to: [$($follower),+$(,$joining)?],
            message_matches: ConsensusNetMessage::Prepare(cp, ticket, prep_view) => {
                round_consensus_proposal = cp.clone();
                round_ticket = ticket.clone();
                view = *prep_view;
            }
        };

        send! {
            description: "Follower - PrepareVote",
            from: [$($follower),+], to: $leader,
            message_matches: ConsensusNetMessage::PrepareVote(_)
        };

        broadcast! {
            description: "Leader - Confirm",
            from: $leader, to: [$($follower),+$(,$joining)?],
            message_matches: ConsensusNetMessage::Confirm(..)
        };

        send! {
            description: "Follower - Confirm Ack",
            from: [$($follower),+], to: $leader,
            message_matches: ConsensusNetMessage::ConfirmAck(_)
        };

        broadcast! {
            description: "Leader - Commit",
            from: $leader, to: [$($follower),+$(,$joining)?],
            message_matches: ConsensusNetMessage::Commit(..)
        };

        (round_consensus_proposal, round_ticket, view)
    }};
}

macro_rules! disseminate {
    (txs: [$($txs:expr),+], owner: $owner:expr, voters: [$($voter:expr),+]) => {{

        let lane_id = LaneId::new($owner.validator_pubkey().clone());
        let dp = $owner.create_data_proposal_on_top(lane_id, &[$($txs),+]);
        $owner
            .process_new_data_proposal(dp.clone())
            .unwrap();
        $owner
            .process_dissemination_events()
            .await
            .expect("process dissemination events");

        let dp_msg = broadcast! {
            description: "Disseminate DataProposal",
            from: $owner, to: [$($voter),+],
            message_matches: MempoolNetMessage::DataProposal(..)
        };

        let mut voters = vec![$(&mut $voter),+];

        join_all(
            voters
                .iter_mut()
                .map(|ctx| ctx.handle_processed_data_proposals()),
        )
        .await;

        let mut votes = Vec::new();
        for voter_idx in 0..voters.len() {
            let vote_msg = {
                let voter = &mut voters[voter_idx];
                let message = voter
                    .assert_broadcast("Disseminated DataProposal Vote")
                    .await;
                match &message.msg {
                    MempoolNetMessage::DataVote(..) => {}
                    _ => {
                        let msg_variant_name: &'static str = message.msg.clone().into();
                        panic!(
                            "[broadcast from: voter] Disseminated DataProposal Vote: Message {msg_variant_name} did not match DataVote"
                        );
                    }
                }
                message
            };

            $owner
                .handle_msg(&vote_msg, "Handling disseminated data proposal vote")
                .await;
            for receiver in voters.iter_mut() {
                receiver
                    .handle_msg(&vote_msg, "Handling disseminated data proposal vote")
                    .await;
            }
            votes.push(vote_msg);
        }

        $owner
            .process_dissemination_events()
            .await
            .expect("process dissemination events");

        (dp, dp_msg, votes)
    }};
}

macro_rules! assert_chanmsg_matches {
    ($chan: expr, $pat:pat => $block:block) => {{
        let var = $chan.try_recv().unwrap().into_message();
        if let $pat = var {
            $block
        } else {
            panic!("Var {:?} should match {}", var, stringify!($pat));
        }
    }};
}

macro_rules! build_nodes {
    ($count:tt) => {{
        async {
            let cryptos: Vec<BlstCrypto> = AutobahnTestCtx::generate_cryptos($count);

            let mut nodes = vec![];

            for i in 0..$count {
                let crypto = cryptos.get(i).unwrap().clone();
                let mut autobahn_node =
                    AutobahnTestCtx::new(format!("node-{i}").as_ref(), crypto).await;

                autobahn_node.consensus_ctx.setup_node(i, &cryptos);
                autobahn_node.mempool_ctx.setup_node(&cryptos).await;
                nodes.push(autobahn_node);
            }

            build_tuple!(nodes.remove(0), $count)
        }
    }};
}

pub(crate) use assert_chanmsg_matches;
pub(crate) use broadcast;
pub(crate) use build_nodes;
pub(crate) use build_tuple;
pub(crate) use disseminate;
pub(crate) use send;
pub(crate) use simple_commit_round;

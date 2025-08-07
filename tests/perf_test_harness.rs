#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
use std::time::Duration;

use anyhow::Result;
use fixtures::{
    ctx::E2ECtx,
    test_helpers::{self, ConfMaker},
};
use hyle::utils::conf::Conf;

mod fixtures;

#[ignore = "This is intended to easily start a few nodes locally for devs"]
#[test_log::test(tokio::test)]
async fn setup_4_nodes() -> Result<()> {
    let mut ctx = E2ECtx::new_multi_with_indexer(4, 1000).await?;

    // To use this harness, comment out the 'ignore' above and run something like:
    // RUST_LOG=perf_test_harness=warn,error cargo test --release --test perf_test_harness -- --nocapture

    ctx.stop_node(3).await?;
    ctx.get_instructions_for(3);

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

#[ignore = "This is intended to easily start a few nodes locally for devs"]
#[test_log::test(tokio::test)]
async fn custom_setup() -> Result<()> {
    std::env::set_var("RISC0_DEV_MODE", "1");

    let mut conf_maker = ConfMaker::default();
    conf_maker.default.consensus.slot_duration = Duration::from_millis(1000);

    let count = 4;
    let mut nodes = {
        let mut nodes = Vec::new();
        let mut confs = Vec::new();

        let default_conf = Conf::new(vec![], None, None).unwrap();

        let mut peers = Vec::new();
        let mut genesis_stakers = std::collections::HashMap::new();

        for i in 0..count {
            let mut node_conf = conf_maker.build("node").await;
            peers.push(node_conf.p2p.public_address.clone());
            genesis_stakers.insert(node_conf.id.clone(), 100);

            // Use predictable ports.
            node_conf.rest_server_port = default_conf.rest_server_port + i;
            node_conf.tcp_server_port = default_conf.tcp_server_port + i;
            node_conf.da_server_port = default_conf.da_server_port + i;
            node_conf.da_public_address = format!("localhost:{}", node_conf.da_server_port);
            // Connect all to the second node
            node_conf.da_read_from = format!("localhost:{}", default_conf.da_server_port + 1);

            node_conf.websocket.enabled = false;

            confs.push(node_conf);
        }

        for node_conf in confs.iter_mut() {
            node_conf.p2p.peers = peers.clone();
            node_conf.genesis.stakers = genesis_stakers.clone();
            let node = test_helpers::TestProcess::new("hyle", node_conf.clone());
            nodes.push(node);
        }
        nodes
    };

    tracing::warn!(
        "🚀 Start the first node with the following command:\nhyle=$(pwd)/target/release/hyle && (cd {} && RUST_LOG=info \"$hyle\")",
        nodes[0].dir.path().display()
    );

    let _n = nodes
        .drain(1..)
        .map(|node| node.start())
        .collect::<Vec<_>>();

    tracing::warn!("🚀 E2E test environment is ready!");

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

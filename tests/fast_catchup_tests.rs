#![allow(clippy::unwrap_used, clippy::expect_used)]

use anyhow::Result;
use fixtures::ctx::E2ECtx;

mod fixtures;

mod e2e_fast_catchup {

    use client_sdk::rest_client::NodeApiClient;

    use crate::fixtures::test_helpers::{self, wait_height};

    use super::*;

    #[test_log::test(tokio::test)]
    async fn fast_catchup_correctly_with_backfill() -> Result<()> {
        let mut ctx = E2ECtx::new_multi_with_indexer(4, 500).await?;

        ctx.wait_height(2).await?;

        _ = ctx.stop_node(3).await;

        let mut conf = ctx.nodes.get(3).expect("Node 3 should exist").conf.clone();

        ctx.wait_height(5).await?;

        conf.run_fast_catchup = true;
        conf.fast_catchup_from = format!(
            "http://localhost:{}",
            ctx.nodes
                .first()
                .expect("Node 0 should exist")
                .conf
                .admin_server_port
        );
        conf.fast_catchup_backfill = true;

        let process = test_helpers::TestProcess::new("hyle", conf);
        process.start();

        let client = ctx.client_by_id("node-3");
        let _ = wait_height(client, 4).await;

        assert!(client.get_block_height().await.unwrap().0 > 10);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn fast_catchup_correctly() -> Result<()> {
        let mut ctx = E2ECtx::new_multi_with_indexer(4, 500).await?;

        ctx.wait_height(2).await?;

        _ = ctx.stop_node(3).await;

        let mut conf = ctx.nodes.get(3).expect("Node 3 should exist").conf.clone();

        ctx.wait_height(5).await?;

        conf.run_fast_catchup = true;
        conf.fast_catchup_from = format!(
            "http://localhost:{}",
            ctx.nodes
                .first()
                .expect("Node 0 should exist")
                .conf
                .admin_server_port
        );
        conf.fast_catchup_backfill = false;

        let process = test_helpers::TestProcess::new("hyle", conf);
        process.start();

        let client = ctx.client_by_id("node-3");
        let _ = wait_height(client, 4).await;

        assert!(client.get_block_height().await.unwrap().0 > 10);

        Ok(())
    }
}

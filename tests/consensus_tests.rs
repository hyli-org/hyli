#![allow(clippy::unwrap_used, clippy::expect_used)]

use hyle_modules::log_error;

use anyhow::Result;
use fixtures::ctx::E2ECtx;

mod fixtures;

mod e2e_consensus {

    use client_sdk::helpers::risc0::Risc0Prover;
    use client_sdk::rest_client::NodeApiClient;
    use client_sdk::transaction_builder::{ProvableBlobTx, TxExecutor, TxExecutorBuilder};
    use fixtures::test_helpers::send_transaction;
    use hydentity::client::tx_executor_handler::{register_identity, verify_identity};
    use hydentity::Hydentity;
    use hyle::genesis::States;
    use hyle_contract_sdk::Identity;
    use hyle_contract_sdk::ZkContract;
    use hyle_contracts::{HYDENTITY_ELF, HYLLAR_ELF, STAKING_ELF};
    use hyle_model::{ContractName, StateCommitment, TxHash};
    use hyllar::client::tx_executor_handler::transfer;
    use hyllar::erc20::ERC20;
    use hyllar::{Hyllar, FAUCET_SECP256K1};
    use staking::client::tx_executor_handler::{delegate, stake};
    use staking::state::Staking;
    use tracing::{info, warn};

    use crate::fixtures::test_helpers::wait_height;

    use super::*;

    #[test_log::test(tokio::test)]
    async fn single_node_generates_blocks() -> Result<()> {
        let ctx = E2ECtx::new_single(50).await?;
        ctx.wait_height(20).await?;
        Ok(())
    }

    #[ignore = "flakky"]
    #[test_log::test(tokio::test)]
    async fn can_run_lot_of_nodes() -> Result<()> {
        let ctx = E2ECtx::new_multi(10, 1000).await?;

        ctx.wait_height(2).await?;

        Ok(())
    }

    async fn scenario_rejoin_common(ctx: &mut E2ECtx, stake_amount: u128) -> Result<()> {
        ctx.wait_height(2).await?;

        let joining_client = ctx.add_node().await?;

        let node_info = joining_client.get_node_info().await?;
        wait_height(joining_client, 2).await?;

        assert!(node_info.pubkey.is_some());

        let hyllar: Hyllar = log_error!(
            ctx.indexer_client()
                .fetch_current_state(&"hyllar".into())
                .await,
            "fetch state failed"
        )
        .unwrap();
        let hydentity: Hydentity = ctx
            .indexer_client()
            .fetch_current_state(&"hydentity".into())
            .await?;

        let staking_state: StateCommitment = StateCommitment(
            ctx.indexer_client()
                .get_indexer_contract(&"staking".into())
                .await?
                .state_commitment,
        );

        let staking: Staking = ctx
            .client()
            .get_consensus_staking_state()
            .await
            .unwrap()
            .into();

        assert_eq!(staking_state, staking.commit());
        let states = States {
            hyllar,
            hydentity,
            staking,
        };

        let mut tx_ctx = TxExecutorBuilder::new(states)
            // Replace prover binaries for non-reproducible mode.
            .with_prover("hydentity".into(), Risc0Prover::new(HYDENTITY_ELF))
            .with_prover("hyllar".into(), Risc0Prover::new(HYLLAR_ELF))
            .with_prover("staking".into(), Risc0Prover::new(STAKING_ELF))
            .build();

        let node_identity = Identity(format!("{}@hydentity", node_info.id));
        {
            let mut transaction = ProvableBlobTx::new(node_identity.clone());

            register_identity(&mut transaction, "hydentity".into(), "password".to_owned())?;

            let tx_hash = send_transaction(ctx.client(), transaction, &mut tx_ctx).await;
            tracing::warn!("Register TX Hash: {:?}", tx_hash);
        }
        {
            let mut transaction = ProvableBlobTx::new(FAUCET_SECP256K1.into());

            verify_identity(
                &mut transaction,
                "hydentity".into(),
                &tx_ctx.hydentity,
                "password".to_string(),
            )
            .expect("verify_identity failed");

            transfer(
                &mut transaction,
                "hyllar".into(),
                node_identity.0.clone(),
                stake_amount,
            )
            .expect("transfer failed");

            let tx_hash = send_transaction(ctx.client(), transaction, &mut tx_ctx).await;
            tracing::warn!("Transfer TX Hash: {:?}", tx_hash);
        }
        {
            let mut transaction = ProvableBlobTx::new(node_identity.clone());

            verify_identity(
                &mut transaction,
                "hydentity".into(),
                &tx_ctx.hydentity,
                "password".to_string(),
            )?;

            stake(&mut transaction, ContractName::new("staking"), stake_amount)?;

            transfer(
                &mut transaction,
                "hyllar".into(),
                "staking".to_string(),
                stake_amount,
            )?;

            delegate(&mut transaction, node_info.pubkey.clone().unwrap())?;

            let tx_hash = send_transaction(ctx.client(), transaction, &mut tx_ctx).await;
            tracing::warn!("staking TX Hash: {:?}", tx_hash);
        }

        // 2 slots to get the tx in a blocks
        // 1 slot to send the candidacy
        // 1 slot to add the validator to consensus
        ctx.wait_height(4).await?;

        let consensus = ctx.client().get_consensus_info().await?;

        assert_eq!(consensus.validators.len(), 3, "expected 3 validators");

        assert!(
            consensus.validators.contains(&node_info.pubkey.unwrap()),
            "node pubkey not found in validators",
        );

        ctx.wait_height(1).await?;
        Ok(())
    }

    async fn gen_txs(
        ctx: &mut E2ECtx,
        tx_ctx: &mut TxExecutor<States>,
        id: String,
        amount: u128,
    ) -> Result<Vec<TxHash>> {
        let mut tx_hashes = vec![];
        let identity = Identity(format!("{}@hydentity", id));
        {
            let mut transaction = ProvableBlobTx::new(identity.clone());

            register_identity(&mut transaction, "hydentity".into(), "password".to_owned())?;

            let tx_hash = send_transaction(ctx.client(), transaction, tx_ctx).await;
            tracing::warn!("Register TX Hash: {:?}", tx_hash);
        }

        {
            let mut transaction = ProvableBlobTx::new("faucet@hydentity".into());

            verify_identity(
                &mut transaction,
                "hydentity".into(),
                &tx_ctx.hydentity,
                "password".to_string(),
            )?;

            transfer(
                &mut transaction,
                "hyllar".into(),
                identity.0.clone(),
                amount,
            )?;

            let tx_hash = send_transaction(ctx.client(), transaction, tx_ctx).await;
            tracing::warn!("Transfer TX Hash: {:?}", tx_hash);
            tx_hashes.push(tx_hash);
        }

        Ok(tx_hashes)
    }

    #[test_log::test(tokio::test)]
    async fn can_rejoin_blocking_consensus() -> Result<()> {
        let mut ctx = E2ECtx::new_multi_with_indexer(2, 500).await?;

        scenario_rejoin_common(&mut ctx, 100).await?;

        // TODO: we should be able to exit the consensus and rejoin it again, but this doesn't work when we block it for now.
        /*
        info!("Stopping node");
        ctx.stop_node(3).await?;

        // Wait for a few seconds
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        info!("Experienced timeouts");
        //info!("Waiting for a few blocks");
        //ctx.wait_height(2).await?;

        // We restart during the timeout timer which is fine.
        ctx.restart_node(3)?;

        ctx.wait_height(4).await?;
        */
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn can_rejoin_not_blocking_consensus() -> Result<()> {
        let mut ctx = E2ECtx::new_multi_with_indexer(2, 500).await?;

        scenario_rejoin_common(&mut ctx, 50).await?;

        info!("Stopping node");
        ctx.stop_node(3).await?;

        info!("Waiting for a few blocks");
        ctx.wait_height(2).await?;

        // We restart during the timeout timer which is fine.
        ctx.restart_node(3)?;

        ctx.wait_height(1).await?;

        Ok(())
    }

    async fn init_states(ctx: &mut E2ECtx) -> TxExecutor<States> {
        let hyllar: Hyllar = ctx
            .indexer_client()
            .fetch_current_state(&"hyllar".into())
            .await
            .unwrap();
        let hydentity: Hydentity = ctx
            .indexer_client()
            .fetch_current_state(&"hydentity".into())
            .await
            .unwrap();

        let states = States {
            hyllar,
            hydentity,
            staking: Staking::default(),
        };

        TxExecutorBuilder::new(states)
            // Replace prover binaries for non-reproducible mode.
            .with_prover("hydentity".into(), Risc0Prover::new(HYDENTITY_ELF))
            .with_prover("hyllar".into(), Risc0Prover::new(HYLLAR_ELF))
            .build()
    }

    #[test_log::test(tokio::test)]
    async fn can_restart_single_node_after_txs() -> Result<()> {
        let mut ctx = E2ECtx::new_single_with_indexer(500).await?;

        ctx.wait_indexer_height(1).await?;

        // Gen a few txs
        let mut tx_ctx = init_states(&mut ctx).await;

        warn!("Starting generating txs");

        for i in 0..2 {
            _ = gen_txs(&mut ctx, &mut tx_ctx, format!("alex{}", i), 100 + i).await?;
        }

        // Wait until it's processed.
        let mut state: Hyllar;
        loop {
            let s: Result<Hyllar> = ctx
                .indexer_client()
                .fetch_current_state(&ContractName::new("hyllar"))
                .await;
            if let Ok(s) = s {
                state = s;
                if state.balance_of("alex1@hydentity").is_ok() {
                    break;
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }

        ctx.stop_node(0).await?;
        ctx.restart_node(0)?;

        ctx.wait_height(0).await?;

        _ = gen_txs(&mut ctx, &mut tx_ctx, format!("alex{}", 2), 100 + 2).await?;

        // Wait until it's processed.
        let mut state: Hyllar;
        loop {
            let s: Result<Hyllar> = ctx
                .indexer_client()
                .fetch_current_state(&ContractName::new("hyllar"))
                .await;
            if let Ok(s) = s {
                state = s;
                if state.balance_of("alex2@hydentity").is_ok() {
                    break;
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }

        // Check everything works out.
        for i in 0..3 {
            let balance = state.balance_of(&format!("alex{}@hydentity", i));
            info!("Checking alex{}@hydentity balance: {:?}", i, balance);
            assert_eq!(balance.unwrap(), ((100 + i) as u128));
        }

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn can_restart_multi_node_after_txs() -> Result<()> {
        let mut ctx = E2ECtx::new_multi_with_indexer_and_timestamp_checks(
            4,
            500,
            hyle::utils::conf::TimestampCheck::Monotonic,
        )
        .await?;

        _ = ctx.wait_indexer_height(1).await;

        // Gen a few txs
        let mut tx_ctx = init_states(&mut ctx).await;

        for i in 0..2 {
            _ = gen_txs(&mut ctx, &mut tx_ctx, format!("alex{}", i), 100 + i).await?;
        }

        // Wait until it's processed.
        let mut state: Hyllar;
        loop {
            let s: Result<Hyllar> = ctx
                .indexer_client()
                .fetch_current_state(&ContractName::new("hyllar"))
                .await;
            if let Ok(s) = s {
                state = s;
                if state.balance_of("alex1@hydentity").is_ok() {
                    break;
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }

        ctx.stop_all().await;

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        warn!("Restarting nodes");

        ctx.restart_node(0)?;
        ctx.restart_node(1)?;
        ctx.restart_node(2)?;
        ctx.restart_node(3)?;
        ctx.restart_node(4)?;

        ctx.wait_height(1).await?;

        _ = gen_txs(&mut ctx, &mut tx_ctx, format!("alex{}", 2), 100 + 2).await?;

        // Wait until it's processed.
        loop {
            let s: Result<Hyllar> = ctx
                .indexer_client()
                .fetch_current_state(&ContractName::new("hyllar"))
                .await;
            if let Ok(s) = s {
                state = s;
                if state.balance_of("alex2@hydentity").is_ok() {
                    break;
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }

        // Check everything works out.
        for i in 0..3 {
            let balance = state.balance_of(&format!("alex{}@hydentity", i));
            info!("Checking alex{}@hydentity balance: {:?}", i, balance);
            assert_eq!(balance.unwrap(), ((100 + i) as u128));
        }

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn multiple_nonconsecutive_timeouts() -> Result<()> {
        let mut ctx = E2ECtx::new_multi(8, 500).await?;
        ctx.stop_node(7).await.unwrap();
        ctx.stop_node(3).await.unwrap();

        _ = ctx.wait_height(1).await;

        warn!("Ready to go");

        _ = ctx.wait_height(8).await;

        Ok(())
    }
    #[test_log::test(tokio::test)]
    async fn multiple_consecutive_timeouts() -> Result<()> {
        let mut ctx = E2ECtx::new_multi(8, 500).await?;
        // These end up being consecutive with crypto pubkey sorting.
        ctx.stop_node(7).await.unwrap();
        ctx.stop_node(1).await.unwrap();

        _ = ctx.wait_height(1).await;

        warn!("Ready to go");

        _ = ctx.wait_height(8).await;

        Ok(())
    }
}

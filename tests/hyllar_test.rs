#![allow(clippy::unwrap_used, clippy::expect_used)]
use fixtures::ctx::E2ECtx;
use tracing::info;

mod fixtures;

use anyhow::Result;

mod e2e_hyllar {
    use client_sdk::{
        contract_states,
        helpers::risc0::Risc0Prover,
        rest_client::NodeApiClient,
        transaction_builder::{ProvableBlobTx, TxExecutorBuilder, TxExecutorHandler},
    };
    use hydentity::{
        client::tx_executor_handler::{register_identity, verify_identity},
        Hydentity,
    };
    use hyle_contract_sdk::{Blob, Calldata, ContractName, HyleOutput};
    use hyle_contracts::{HYDENTITY_ELF, HYLLAR_ELF};
    use hyllar::{client::tx_executor_handler::transfer, erc20::ERC20, Hyllar, FAUCET_ID};

    use super::*;

    contract_states!(
        struct States {
            hydentity: Hydentity,
            hyllar: Hyllar,
        }
    );

    async fn scenario_hyllar(ctx: E2ECtx) -> Result<E2ECtx> {
        info!("➡️  Setting up the executor with the initial state");

        let hydentity: hydentity::Hydentity = ctx
            .indexer_client()
            .fetch_current_state(&"hydentity".into())
            .await?;
        let hyllar: Hyllar = ctx
            .indexer_client()
            .fetch_current_state(&"hyllar".into())
            .await?;
        let mut executor = TxExecutorBuilder::new(States { hydentity, hyllar })
            // Replace prover binaries for non-reproducible mode.
            .with_prover("hydentity".into(), Risc0Prover::new(HYDENTITY_ELF))
            .with_prover("hyllar".into(), Risc0Prover::new(HYLLAR_ELF))
            .build();

        info!("➡️  Sending blob to register bob identity");

        let mut tx = ProvableBlobTx::new("bob@hydentity".into());
        register_identity(&mut tx, "hydentity".into(), "password".to_string())?;
        ctx.send_provable_blob_tx(&tx).await?;

        let tx = executor.process(tx)?;
        let proof = tx.iter_prove().next().unwrap().await?;

        info!("➡️  Sending proof for register");
        ctx.send_proof_single(proof).await?;

        info!("➡️  Waiting for height 2");
        ctx.wait_height(2).await?;

        info!("Hydentity: {:?}", executor.hydentity);

        info!("➡️  Sending blob to transfer 25 tokens from faucet to bob");

        let mut tx = ProvableBlobTx::new(FAUCET_ID.into());

        verify_identity(
            &mut tx,
            "hydentity".into(),
            &executor.hydentity,
            "password".to_string(),
        )?;

        transfer(&mut tx, "hyllar".into(), "bob@hydentity".to_string(), 25)?;

        ctx.send_provable_blob_tx(&tx).await?;

        let tx = executor.process(tx)?;
        let mut proofs = tx.iter_prove();

        let hydentity_proof = proofs.next().unwrap().await?;
        let hyllar_proof = proofs.next().unwrap().await?;

        info!("➡️  Sending proof for hydentity");
        ctx.send_proof_single(hydentity_proof).await?;

        info!("➡️  Sending proof for hyllar");
        ctx.send_proof_single(hyllar_proof).await?;

        info!("➡️  Waiting for height 5 on indexer");
        ctx.wait_indexer_height(5).await?;

        let state: Hyllar = ctx
            .indexer_client()
            .fetch_current_state(&"hyllar".into())
            .await?;
        assert_eq!(
            state
                .balance_of("bob@hydentity")
                .expect("bob identity not found"),
            25
        );
        Ok(ctx)
    }

    #[test_log::test(tokio::test)]
    async fn hyllar_single_node() -> Result<()> {
        let ctx = E2ECtx::new_single_with_indexer(500).await?;
        scenario_hyllar(ctx).await?;
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn hyllar_multi_nodes() -> Result<()> {
        let ctx = E2ECtx::new_multi_with_indexer(2, 500).await?;

        let node = ctx.client().get_node_info().await?;
        let staking = ctx.client().get_consensus_staking_state().await?;
        let initial_balance = staking
            .fees
            .balances
            .get(node.pubkey.as_ref().unwrap())
            .expect("balance");

        let ctx = scenario_hyllar(ctx).await?;

        let staking = ctx.client().get_consensus_staking_state().await?;
        let balance = staking
            .fees
            .balances
            .get(node.pubkey.as_ref().unwrap())
            .expect("balance");
        assert!(balance.cumul_size.0 > initial_balance.cumul_size.0);
        assert!(balance.balance < initial_balance.balance);

        Ok(())
    }
}

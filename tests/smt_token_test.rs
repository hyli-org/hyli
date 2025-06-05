#![allow(clippy::unwrap_used, clippy::expect_used)]
use fixtures::ctx::E2ECtx;
use tracing::info;

mod fixtures;

use anyhow::Result;

mod e2e_smt_token {
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
    use hyle_contracts::{HYDENTITY_ELF, SMT_TOKEN_ELF};
    use smt_token::{client::tx_executor_handler::SmtTokenProvableState, FAUCET_ID};

    use super::*;

    contract_states!(
        struct States {
            hydentity: Hydentity,
            oranj: SmtTokenProvableState,
        }
    );

    async fn scenario_smt_token(ctx: E2ECtx) -> Result<E2ECtx> {
        info!("➡️  Setting up the executor with the initial state");

        let hydentity: hydentity::Hydentity = ctx
            .indexer_client()
            .fetch_current_state(&"hydentity".into())
            .await?;
        // TODO: indexer toutes les transactions hihi
        // let token: SmtToken = ctx
        //     .indexer_client()
        //     .fetch_current_state(&"oranj".into())
        //     .await?;
        let oranj = SmtTokenProvableState::default();

        let mut executor = TxExecutorBuilder::new(States { hydentity, oranj })
            // Replace prover binaries for non-reproducible mode.
            .with_prover(
                "hydentity".into(),
                Risc0Prover::new(HYDENTITY_ELF, "hydentity".into()),
            )
            .with_prover(
                "oranj".into(),
                Risc0Prover::new(SMT_TOKEN_ELF, "oranj".into()),
            )
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

        executor.oranj.transfer(
            &mut tx,
            "oranj".into(),
            FAUCET_ID.into(),
            "bob@hydentity".into(),
            25,
        )?;

        ctx.send_provable_blob_tx(&tx).await?;

        let tx = executor.process(tx)?;
        let mut proofs = tx.iter_prove();

        let hydentity_proof = proofs.next().unwrap().await?;
        let smt_token_proof = proofs.next().unwrap().await?;

        info!("➡️  Sending proof for hydentity");
        ctx.send_proof_single(hydentity_proof).await?;

        info!("➡️  Sending proof for smt_token");
        ctx.send_proof_single(smt_token_proof).await?;

        info!("➡️  Waiting for height 5");
        ctx.wait_height(5).await?;

        // let state: SmtToken = ctx
        //     .indexer_client()
        //     .fetch_current_state(&"oranj".into())
        //     .await?;
        let state = executor.oranj.get_state();
        assert_eq!(
            state
                .get(&"bob@hydentity".into())
                .expect("bob identity not found")
                .balance,
            25
        );

        Ok(ctx)
    }

    #[ignore = "need new_single_with_indexer"]
    #[test_log::test(tokio::test)]
    async fn smt_token_single_node() -> Result<()> {
        let ctx = E2ECtx::new_single(500).await?;
        scenario_smt_token(ctx).await?;
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn smt_token_multi_nodes() -> Result<()> {
        let ctx = E2ECtx::new_multi_with_indexer(2, 500).await?;

        let node = ctx.client().get_node_info().await?;
        let staking = ctx.client().get_consensus_staking_state().await?;
        let initial_balance = staking
            .fees
            .balances
            .get(node.pubkey.as_ref().unwrap())
            .expect("balance");

        let ctx = scenario_smt_token(ctx).await?;

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

#![allow(clippy::unwrap_used, clippy::expect_used)]

use anyhow::Result;
use client_sdk::{
    helpers::risc0::Risc0Prover,
    rest_client::{NodeApiClient, NodeApiHttpClient},
    transaction_builder::{ProvableBlobTx, TxExecutorBuilder},
};
use fixtures::{ctx::E2ECtx, test_helpers::send_transaction};
use hydentity::{
    client::tx_executor_handler::{register_identity, verify_identity},
    Hydentity,
};
use hyle::genesis::States;
use hyle_contracts::{HYDENTITY_ELF, HYLLAR_ELF, STAKING_ELF};
use hyle_model::{api::APIRegisterContract, ContractName, Identity, ProgramId, StateCommitment};
use hyllar::{client::tx_executor_handler::transfer, Hyllar, FAUCET_ID};
use staking::{
    client::tx_executor_handler::{delegate, deposit_for_fees, stake},
    state::Staking,
};

mod fixtures;

async fn faucet_and_delegate(
    ctx: &E2ECtx,
    client: &NodeApiHttpClient,
    stake_amount: u128,
) -> Result<()> {
    let hyllar: Hyllar = ctx
        .indexer_client()
        .fetch_current_state(&"hyllar".into())
        .await?;
    let hydentity: Hydentity = ctx
        .indexer_client()
        .fetch_current_state(&"hydentity".into())
        .await?;

    let staking: Staking = ctx.client().get_consensus_staking_state().await?.into();

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

    let node_info = client.get_node_info().await?;

    let node_identity = Identity(format!("{}@hydentity", node_info.id));
    {
        let mut transaction = ProvableBlobTx::new(node_identity.clone());

        register_identity(&mut transaction, "hydentity".into(), "password".to_owned())?;

        let tx_hash = send_transaction(ctx.client(), transaction, &mut tx_ctx).await;
        tracing::warn!("Register TX Hash: {:?}", tx_hash);
    }
    {
        let mut transaction = ProvableBlobTx::new(FAUCET_ID.into());

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
            1000000000,
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

        // Deposit for fees
        deposit_for_fees(
            &mut transaction,
            ContractName::new("staking"),
            node_info.pubkey.clone().unwrap(),
            1_000_000,
        )?;

        transfer(
            &mut transaction,
            ContractName::new("hyllar"),
            "staking".to_string(),
            1_000_000,
        )?;

        let tx_hash = send_transaction(ctx.client(), transaction, &mut tx_ctx).await;
        tracing::warn!("staking TX Hash: {:?}", tx_hash);
    }
    Ok(())
}

async fn scenario_lane_manager_outside_consensus(mut ctx: E2ECtx, delegate: bool) -> Result<()> {
    let mut conf = ctx.make_conf("lane_mgr").await;
    conf.p2p.mode = hyle::utils::conf::P2pMode::LaneManager;

    ctx.add_node_with_conf(conf).await?;
    let lane_mgr_client = ctx.client_by_id("lane_mgr-1");

    if delegate {
        faucet_and_delegate(&ctx, lane_mgr_client, 100).await?;
    }

    // Need to wait for both nodes to have caught up node state.
    let _ = ctx.wait_height(2).await;

    let tx_hash = lane_mgr_client
        .register_contract(APIRegisterContract {
            verifier: "test".into(),
            program_id: ProgramId(vec![1, 2, 3]),
            state_commitment: StateCommitment(vec![1, 2, 3]),
            contract_name: ContractName::new("test"),
            ..Default::default()
        })
        .await?;

    let indexer_client = ctx.indexer_client();

    loop {
        match indexer_client.get_transaction_with_hash(&tx_hash).await {
            Ok(_) => {
                break;
            }
            Err(_) => {
                std::thread::sleep(std::time::Duration::from_millis(200));
            }
        }
    }

    assert_eq!(
        indexer_client
            .get_indexer_contract(&ContractName::new("test"))
            .await
            .unwrap()
            .program_id,
        vec![1, 2, 3]
    );

    Ok(())
}

#[test_log::test(tokio::test)]
async fn lane_manager_outside_consensus_single_node() -> Result<()> {
    let ctx = E2ECtx::new_single_with_indexer(500).await?;
    scenario_lane_manager_outside_consensus(ctx, false).await
}

#[test_log::test(tokio::test)]
async fn lane_manager_outside_consensus_multi_node() -> Result<()> {
    let ctx = E2ECtx::new_multi_with_indexer(2, 500).await?;
    scenario_lane_manager_outside_consensus(ctx, true).await
}

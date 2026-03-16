use anyhow::{Context, Result};
use client_sdk::rest_client::{NodeApiClient, NodeApiHttpClient};
use sdk::{
    api::APIRegisterContract, ContractName, ProgramId, StateCommitment, Verifier, ZkContract,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

use crate::{server::conf::Conf, HyperlaneBridgeState};

pub mod metadata {
    /// Program ID of the compiled risc0 guest (placeholder until the guest is compiled).
    pub const BRIDGE_ID: [u8; 32] = sdk::str_to_u8(include_str!("../../hyperlane-bridge.txt"));
}

pub async fn init_contracts(conf: &Conf, node: Arc<NodeApiHttpClient>) -> Result<()> {
    let bridge_cn = ContractName(conf.bridge_cn.clone());
    let hyperlane_cn = ContractName(conf.hyperlane_cn.clone());

    // ── Contract A: HyperlaneBridgeState (risc0) ──────────────────────────────
    let bridge_state = HyperlaneBridgeState {
        hyperlane_contract: hyperlane_cn.clone(),
        token_contract: ContractName(conf.token_cn.clone()),
    };

    match node.get_contract(bridge_cn.clone()).await {
        Ok(_) => info!(
            "Contract '{}' already exists, skipping registration",
            bridge_cn
        ),
        Err(_) => {
            info!("Registering contract '{}'...", bridge_cn);
            node.register_contract(APIRegisterContract {
                verifier: Verifier("risc0-3".to_string()),
                program_id: ProgramId(metadata::BRIDGE_ID.to_vec()),
                state_commitment: bridge_state.commit(),
                contract_name: bridge_cn.clone(),
                timeout_window: None,
                constructor_metadata: None,
            })
            .await
            .context("Registering hyperlane-bridge contract")?;

            wait_for_contract(node.clone(), &bridge_cn).await?;
            info!("Contract '{}' registered successfully", bridge_cn);
        }
    }

    // ── Contract B: hyperlane (reth verifier) ─────────────────────────────────
    let eth_state_root_hex = conf.eth_state_root.trim_start_matches("0x");
    let eth_state_root =
        hex::decode(eth_state_root_hex).context("Decoding eth_state_root hex")?;

    match node.get_contract(hyperlane_cn.clone()).await {
        Ok(_) => info!(
            "Contract '{}' already exists, skipping registration",
            hyperlane_cn
        ),
        Err(_) => {
            info!("Registering contract '{}'...", hyperlane_cn);
            node.register_contract(APIRegisterContract {
                verifier: Verifier(hyli_model::verifiers::RETH.to_string()),
                // reth verifier ignores program_id when signer ≠ program_id
                program_id: ProgramId(vec![0u8; 65]),
                state_commitment: StateCommitment(eth_state_root),
                contract_name: hyperlane_cn.clone(),
                timeout_window: None,
                constructor_metadata: None,
            })
            .await
            .context("Registering hyperlane contract")?;

            wait_for_contract(node.clone(), &hyperlane_cn).await?;
            info!("Contract '{}' registered successfully", hyperlane_cn);
        }
    }

    Ok(())
}

async fn wait_for_contract(node: Arc<NodeApiHttpClient>, name: &ContractName) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if node.get_contract(name.clone()).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .with_context(|| format!("Timeout waiting for contract '{name}'"))
}

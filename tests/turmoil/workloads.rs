//! Test workloads for turmoil simulations.
//!
//! These are the actual test functions that run during simulations,
//! performing operations like submitting transactions and verifying state.

use std::time::Duration;

use anyhow::{bail, ensure};
use client_sdk::rest_client::NodeApiClient;
use hyli_modules::log_error;
use tracing::warn;

use crate::fixtures::{test_helpers::wait_height, turmoil::TurmoilHost};

use super::common::make_register_contract_tx;

/// **Test**
///
/// Inject 10 contracts on node-1.
/// Check on the node (all of them) that all 10 contracts are here.
pub async fn submit_10_contracts(node: TurmoilHost) -> anyhow::Result<()> {
    let client_with_retries = node.client.retry_15times_1000ms();
    let client_no_retry = node.client.with_retry(0, Duration::from_millis(0));

    _ = wait_height(&client_with_retries, 1).await;

    if node.conf.id == "node-1" {
        for i in 1..10 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let tx = make_register_contract_tx(format!("contract-{}", i).into());

            _ = log_error!(
                client_with_retries.send_tx_blob(tx).await,
                "Sending tx blob"
            );
        }
    } else {
        tokio::time::sleep(Duration::from_secs(10)).await;
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    for i in 1..10 {
        let name = format!("contract-{}", i);
        let mut attempts = 0;
        loop {
            if tokio::time::Instant::now() >= deadline {
                bail!("Timed out waiting for contract {}", name);
            }

            match client_no_retry.get_contract(name.clone().into()).await {
                Ok(contract) => {
                    assert_eq!(contract.contract_name.0, name.as_str());
                    break;
                }
                Err(e) => {
                    attempts += 1;
                    if attempts > 20 {
                        return Err(e);
                    }
                    warn!("Retrying get_contract {} attempt {}: {}", name, attempts, e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    Ok(())
}

/// **Test**
///
/// Ensure a timeout view split stalls the network, then recovery resumes after healing.
pub async fn timeout_split_view_recovery(node: TurmoilHost) -> anyhow::Result<()> {
    let client_with_retries = node.client.retry_15times_1000ms();

    _ = wait_height(&client_with_retries, 1).await;

    if node.conf.id != "node-1" {
        tokio::time::sleep(Duration::from_secs(35)).await;
        return Ok(());
    }

    tokio::time::sleep(Duration::from_secs(8)).await;
    let before = client_with_retries.get_block_height().await?.0;
    tracing::info!("Timeout split view: height before stall check: {}", before);

    tokio::time::sleep(Duration::from_secs(6)).await;
    let during = client_with_retries.get_block_height().await?.0;
    tracing::info!("Timeout split view: height during stall check: {}", during);
    ensure!(
        during == before,
        "expected a stalled height during view split"
    );

    tokio::time::sleep(Duration::from_secs(15)).await;
    let after = client_with_retries.get_block_height().await?.0;
    tracing::info!("Timeout split view: height after healing: {}", after);
    ensure!(after > during, "expected height to advance after healing");

    Ok(())
}

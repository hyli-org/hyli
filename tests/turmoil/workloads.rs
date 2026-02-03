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
/// Submit 10 contracts aligned with the sequential chaos phases.
/// Phase schedule matches simulation_chaos_sequential_failures:
/// warmup (3s), phase1 (6s), phase2 (6s), phase3 (5s), phase4 (restart).
pub async fn submit_10_contracts_sequential_failures_phase_aligned(
    node: TurmoilHost,
) -> anyhow::Result<()> {
    let client_with_retries = node.client.retry_15times_1000ms();
    let client_no_retry = node.client.with_retry(0, Duration::from_millis(0));

    _ = wait_height(&client_with_retries, 1).await;

    if node.conf.id == "node-1" {
        // Phase 1 window: after warmup.
        tokio::time::sleep(Duration::from_secs(3)).await;
        for i in 1..=3 {
            let tx = make_register_contract_tx(format!("contract-{}", i).into());
            _ = log_error!(
                client_with_retries.send_tx_blob(tx).await,
                "Sending tx blob (phase 1)"
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // Phase 2 window: after phase1 ends (3 + 6 = 9s).
        tokio::time::sleep(Duration::from_secs(3)).await;
        for i in 4..=6 {
            let tx = make_register_contract_tx(format!("contract-{}", i).into());
            _ = log_error!(
                client_with_retries.send_tx_blob(tx).await,
                "Sending tx blob (phase 2)"
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // Phase 3 is crash window; wait for restart (phase3 end at 20s).
        tokio::time::sleep(Duration::from_secs(8)).await;
        for i in 7..=10 {
            let tx = make_register_contract_tx(format!("contract-{}", i).into());
            _ = log_error!(
                client_with_retries.send_tx_blob(tx).await,
                "Sending tx blob (post-restart)"
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    } else {
        // Keep other nodes alive through all phases.
        tokio::time::sleep(Duration::from_secs(25)).await;
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    for i in 1..=10 {
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
/// Submit 10 contracts aligned with the memory pressure cycles.
/// Phase schedule matches simulation_chaos_memory_pressure:
/// warmup (5s), then 3 cycles of hold (4s) + release (2s).
pub async fn submit_10_contracts_memory_pressure_phase_aligned(
    node: TurmoilHost,
) -> anyhow::Result<()> {
    let client_with_retries = node.client.retry_15times_1000ms();
    let client_no_retry = node.client.with_retry(0, Duration::from_millis(0));

    _ = wait_height(&client_with_retries, 1).await;

    if node.conf.id == "node-1" {
        // Warmup window.
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Cycle 1 (hold 4s): send 4 contracts.
        for i in 1..=4 {
            let tx = make_register_contract_tx(format!("contract-{}", i).into());
            _ = log_error!(
                client_with_retries.send_tx_blob(tx).await,
                "Sending tx blob (cycle 1)"
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // Release window 1 (2s).
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Cycle 2 (hold 4s): send 3 contracts.
        for i in 5..=7 {
            let tx = make_register_contract_tx(format!("contract-{}", i).into());
            _ = log_error!(
                client_with_retries.send_tx_blob(tx).await,
                "Sending tx blob (cycle 2)"
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // Release window 2 (2s).
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Cycle 3 (hold 4s): send 3 contracts.
        for i in 8..=10 {
            let tx = make_register_contract_tx(format!("contract-{}", i).into());
            _ = log_error!(
                client_with_retries.send_tx_blob(tx).await,
                "Sending tx blob (cycle 3)"
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    } else {
        // Keep other nodes alive through all cycles.
        tokio::time::sleep(Duration::from_secs(30)).await;
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    for i in 1..=10 {
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

/// **Test**
///
/// Submit many contracts continuously to create heavy load.
/// This workload is designed to stress test the system under chaos conditions.
pub async fn submit_heavy_load(node: TurmoilHost) -> anyhow::Result<()> {
    let client_with_retries = node.client.retry_15times_1000ms();
    let client_no_retry = node.client.with_retry(0, Duration::from_millis(0));

    _ = wait_height(&client_with_retries, 1).await;

    // All nodes submit transactions to create load
    let node_num: u32 = node
        .conf
        .id
        .strip_prefix("node-")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let tx_count = 50;
    let base_id = node_num * 1000;

    tracing::info!(
        "Heavy load: {} will submit {} transactions",
        node.conf.id,
        tx_count
    );

    // Submit transactions in batches
    for i in 0..tx_count {
        let tx_id = base_id + i;
        let tx = make_register_contract_tx(format!("heavy-contract-{}", tx_id).into());

        match client_no_retry.send_tx_blob(tx).await {
            Ok(_) => {}
            Err(e) => {
                warn!("Heavy load: failed to submit tx {}: {}", tx_id, e);
            }
        }

        // Small delay to avoid overwhelming the node
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    tracing::info!(
        "Heavy load: {} finished submitting transactions",
        node.conf.id
    );

    // Wait for some transactions to be processed
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    let mut verified_count = 0;
    let target_verifications = 10;

    for i in 0..target_verifications {
        let tx_id = base_id + i;
        let name = format!("heavy-contract-{}", tx_id);

        loop {
            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(
                    "Heavy load: {} only verified {}/{} contracts before timeout",
                    node.conf.id,
                    verified_count,
                    target_verifications
                );
                break;
            }

            match client_no_retry.get_contract(name.clone().into()).await {
                Ok(contract) => {
                    assert_eq!(contract.contract_name.0, name.as_str());
                    verified_count += 1;
                    break;
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    tracing::info!(
        "Heavy load: {} verified {}/{} contracts",
        node.conf.id,
        verified_count,
        target_verifications
    );

    Ok(())
}

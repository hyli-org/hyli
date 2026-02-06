use crate::config::HylixConfig;
use crate::constants;
use crate::error::{HylixError, HylixResult};
use client_sdk::rest_client::NodeApiClient;
use std::time::Duration;

use super::context::DevnetContext;

/// Check if devnet is already running
pub async fn is_devnet_responding(context: &DevnetContext) -> HylixResult<bool> {
    // Use tokio::time::timeout to add a timeout to the async call
    match tokio::time::timeout(Duration::from_secs(5), context.client.get_block_height()).await {
        Ok(result) => match result {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        },
        Err(_) => Ok(false),
    }
}

/// Wait for block height to reach a specific value
pub async fn wait_for_block_height(
    pb: &indicatif::ProgressBar,
    context: &DevnetContext,
    target_height: u64,
) -> HylixResult<()> {
    let max_attempts = 60; // 1 minutes with 1-second intervals
    let mut attempts = 0;

    while attempts < max_attempts {
        match tokio::time::timeout(Duration::from_secs(5), context.client.get_block_height()).await
        {
            Ok(result) => match result {
                Ok(block_height) => {
                    let current_height = block_height.0;

                    if current_height >= target_height {
                        pb.set_message("Hyli node started successfully");
                        return Ok(());
                    }
                }
                Err(_) => {
                    // Continue waiting
                }
            },
            Err(_) => {
                pb.set_message(format!(
                    "Timeout getting block height, retrying... (attempt {attempts}/{max_attempts})"
                ));
                // Continue waiting
            }
        }

        attempts += 1;
        if attempts < max_attempts {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    Err(HylixError::devnet(format!(
        "Failed to reach block height {target_height} after {max_attempts} attempts"
    )))
}

/// Wait for the postgres server to be ready
pub async fn wait_for_postgres_server(pb: &indicatif::ProgressBar) -> HylixResult<()> {
    let mut attempts = 0;
    let max_attempts = 60;

    while attempts < max_attempts {
        pb.set_message("Waiting for postgres server to be ready...");
        let mut output = tokio::process::Command::new("docker")
            .args(["exec", "-it", constants::containers::POSTGRES, "pg_isready"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .map_err(|e| HylixError::process(format!("Failed to start Docker container: {e}")))?;

        if output.wait().await?.success() {
            return Ok(());
        }

        attempts += 1;
        if attempts < max_attempts {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    Err(HylixError::devnet(
        "Postgres server did not become ready in time",
    ))
}

/// Check required dependencies
pub fn check_required_dependencies() -> HylixResult<()> {
    let mut missing_deps = Vec::new();

    if which::which("docker").is_err() {
        missing_deps.push("Docker");
    }
    if which::which("npx").is_err() {
        missing_deps.push("npx (Node.js)");
    }

    if !missing_deps.is_empty() {
        return Err(HylixError::devnet(format!(
            "Hylix requires the following binaries to be installed: {}",
            missing_deps.join(", ")
        )));
    }

    // Check if Docker can be executed with proper permissions
    if let Ok(output) = std::process::Command::new("docker")
        .args(["ps"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .output()
    {
        if !output.status.success() {
            return Err(HylixError::devnet(
                "Docker is installed but cannot be executed. Please ensure you have proper permissions to run Docker commands.".to_string(),
            ));
        }
    } else {
        return Err(HylixError::devnet(
            "Failed to execute Docker command. Please ensure Docker daemon is running and you have proper permissions.".to_string(),
        ));
    }

    Ok(())
}

/// Print environment variables for sourcing in bash
pub fn print_devnet_env_vars(config: &HylixConfig) -> HylixResult<()> {
    let devnet = &config.devnet;

    println!("# Hyli devnet environment variables");
    println!("# Source this file in your bash shell: source <(hy devnet env)");
    println!();

    // Node and DA endpoints
    println!(
        "export {}=\"http://localhost:{}\"",
        constants::env_vars::HYLI_NODE_URL,
        devnet.node_port
    );
    println!(
        "export {}=\"localhost:{}\"",
        constants::env_vars::HYLI_DA_READ_FROM,
        devnet.da_port
    );

    // Indexer endpoint
    println!(
        "export {}=\"http://localhost:{}\"",
        constants::env_vars::HYLI_INDEXER_URL,
        devnet.indexer_port
    );

    // Registry endpoints
    println!(
        "export {}=\"http://localhost:{}\"",
        constants::env_vars::HYLI_REGISTRY_URL,
        devnet.registry_server_port
    );
    println!(
        "export {}=\"{}\"",
        constants::env_vars::HYLI_REGISTRY_API_KEY,
        constants::env_values::REGISTRY_API_KEY_DEV
    );
    println!(
        "export HYLI_REGISTRY_UI_URL=\"http://localhost:{}\"",
        devnet.registry_ui_port
    );

    // Wallet endpoints
    println!(
        "export HYLI_WALLET_API_URL=\"http://localhost:{}\"",
        devnet.wallet_api_port
    );
    println!(
        "export HYLI_WALLET_WS_URL=\"ws://localhost:{}\"",
        devnet.wallet_ws_port
    );
    println!(
        "export HYLI_WALLET_UI_URL=\"http://localhost:{}\"",
        devnet.wallet_ui_port
    );

    // Database endpoint
    println!(
        "export {}=\"postgresql://postgres:postgres@localhost:{}\"",
        constants::env_vars::HYLI_DATABASE_URL,
        devnet.postgres_port
    );

    // Explorer URL
    println!("export HYLI_EXPLORER_URL=\"https://explorer.hyli.org/?network=localhost&indexer={}&node={}&wallet={}\"",
             devnet.indexer_port, devnet.node_port, devnet.wallet_api_port);

    // Development mode flags
    println!(
        "export {}=\"{}\"",
        constants::env_vars::RISC0_DEV_MODE,
        constants::env_values::RISC0_DEV_MODE_ONE
    );
    println!(
        "export {}=\"{}\"",
        constants::env_vars::SP1_PROVER,
        constants::env_values::SP1_PROVER_MOCK
    );

    println!();
    println!("# Usage examples:");
    println!("#   source <(hy devnet env)");
    println!("#   echo $HYLI_NODE_URL");
    println!("#   curl $HYLI_NODE_URL/swagger-ui");

    Ok(())
}

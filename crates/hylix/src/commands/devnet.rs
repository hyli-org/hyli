use crate::config::HylixConfig;
use crate::error::{HylixError, HylixResult};
use crate::logging::{create_progress_bar, log_info, log_success, log_warning};
use client_sdk::rest_client::{NodeApiClient, NodeApiHttpClient};
use std::time::Duration;

/// Devnet action enum
#[derive(Debug, Clone)]
pub enum DevnetAction {
    Start { reset: bool },
    Stop,
    Restart { reset: bool },
    Fork { endpoint: String },
}

/// Context struct containing client and config for devnet operations
pub struct DevnetContext {
    pub client: NodeApiHttpClient,
    pub config: HylixConfig,
}

impl DevnetContext {
    /// Create a new DevnetContext
    pub fn new(config: HylixConfig) -> HylixResult<Self> {
        let node_url = format!("http://localhost:{}", config.devnet.node_port);
        let client = NodeApiHttpClient::new(node_url)?;
        Ok(Self { client, config })
    }
}

/// Execute the `hy devnet` command
pub async fn execute(action: DevnetAction) -> HylixResult<()> {
    // Load configuration once
    let config = HylixConfig::load()?;
    let context = DevnetContext::new(config)?;

    match action {
        DevnetAction::Start { reset } => {
            if is_devnet_running(&context).await? {
                log_info("Devnet is already running");
                return Ok(());
            }
            start_devnet(reset, &context).await?;
        }
        DevnetAction::Stop => {
            stop_devnet(&context).await?;
        }
        DevnetAction::Restart { reset } => {
            restart_devnet(reset, &context).await?;
        }
        DevnetAction::Fork { endpoint } => {
            fork_devnet(&endpoint).await?;
        }
    }

    Ok(())
}

/// Start the local devnet
async fn start_devnet(reset: bool, context: &DevnetContext) -> HylixResult<()> {
    let pb = create_progress_bar("Starting local devnet...");
    if reset {
        reset_devnet_state(context).await?;
    }

    create_docker_network(&pb).await?;

    // Start the local node
    start_local_node(&pb, context).await?;

    // Start indexer
    start_indexer(&pb, context).await?;

    // Setup wallet app
    start_wallet_app(&pb, context).await?;

    // Create pre-funded test accounts
    create_test_accounts(&pb, context).await?;

    pb.finish_and_clear();

    log_success("Local devnet started successfully!");
    log_info("Services are running in Docker containers:");
    log_info("  - hyli-devnet-node");
    log_info("  - hyli-devnet-postgres");
    log_info("  - hyli-devnet-indexer");
    log_info("  - hyli-devnet-wallet");
    log_info("Services available at:");
    log_info(&format!(
        "  Node: http://localhost:{}/swagger-ui",
        context.config.devnet.node_port
    ));
    log_info(&format!(
        "  Explorer: https://explorer.hyli.org/?network=localhost&indexer={}&node={}&wallet={}",
        context.config.devnet.indexer_port,
        context.config.devnet.node_port,
        context.config.devnet.wallet_api_port
    ));
    log_info(&format!(
        "  Indexer: http://localhost:{}/swagger-ui",
        context.config.devnet.indexer_port
    ));
    log_info(&format!(
        "  Wallet API: http://localhost:{}/swagger-ui",
        context.config.devnet.wallet_api_port
    ));
    log_info(&format!(
        "  Wallet UI: http://localhost:{}",
        context.config.devnet.wallet_ui_port
    ));

    Ok(())
}

/// Stop the local devnet
async fn stop_devnet(_context: &DevnetContext) -> HylixResult<()> {
    let pb = create_progress_bar("Stopping local devnet...");

    // Stop wallet app
    pb.set_message("Stopping wallet app...");
    stop_wallet_app(&pb).await?;

    // Stop indexer
    pb.set_message("Stopping indexer...");
    stop_indexer(&pb).await?;

    // Stop local node
    pb.set_message("Stopping local node...");
    stop_local_node(&pb).await?;

    // Remove docker network
    pb.set_message("Removing docker network...");
    remove_docker_network(&pb).await?;

    pb.finish_and_clear();
    log_success("Local devnet stopped successfully!");
    Ok(())
}

/// Restart the local devnet
async fn restart_devnet(reset: bool, context: &DevnetContext) -> HylixResult<()> {
    stop_devnet(context).await?;
    start_devnet(reset, context).await?;
    Ok(())
}

/// Fork a running network
async fn fork_devnet(endpoint: &str) -> HylixResult<()> {
    let pb = create_progress_bar(&format!("Forking network at: {}", endpoint));

    // TODO: Implement network forking
    // This would involve:
    // 1. Connecting to the specified endpoint
    // 2. Downloading the current state
    // 3. Setting up a local node with that state
    // 4. Starting all the devnet services

    pb.finish_with_message("Network forking is coming soon!");
    log_info("This feature will allow you to fork any running Hyli network for testing.");

    Ok(())
}

/// Check if devnet is already running
async fn is_devnet_running(context: &DevnetContext) -> HylixResult<bool> {
    // Use tokio::time::timeout to add a timeout to the async call
    match tokio::time::timeout(Duration::from_secs(5), context.client.get_block_height()).await {
        Ok(result) => match result {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        },
        Err(_) => Ok(false),
    }
}

/// Reset devnet state
async fn reset_devnet_state(_context: &DevnetContext) -> HylixResult<()> {
    // TODO: Implement state reset
    // This would involve:
    // 1. Stopping all services
    // 2. Clearing the database
    // 3. Removing any cached data
    // 4. Starting fresh services

    Ok(())
}

/// Create the docker network
async fn create_docker_network(pb: &indicatif::ProgressBar) -> HylixResult<()> {
    use tokio::process::Command;

    pb.set_message("Creating docker network...");

    let output = Command::new("docker")
        .args(["network", "create", "hyli-devnet"])
        .output()
        .await
        .map_err(|e| HylixError::process(format!("Failed to create Docker network: {}", e)))?;

    if !output.status.success() {
        log_warning(&format!("{}", String::from_utf8_lossy(&output.stderr)));
    } else {
        pb.set_message("Docker network created successfully");
    }

    Ok(())
}

/// Start the local node
async fn start_local_node(pb: &indicatif::ProgressBar, context: &DevnetContext) -> HylixResult<()> {
    use tokio::process::Command;
    let image = format!("ghcr.io/hyli-org/hyli:{}", context.config.devnet.version);

    pull_docker_image(pb, &image).await?;

    pb.set_message("Starting Hyli node with Docker...");

    let output = Command::new("docker")
        .args([
            "run",
            "-d",
            "--network",
            "hyli-devnet",
            "--name",
            "hyli-devnet-node",
            "-e",
            "HYLI_RUN_INDEXER=false",
            "-e",
            "HYLI_RUN_EXPLORER=false",
            "-p",
            &format!("{}:4321", context.config.devnet.node_port),
            "-p",
            &format!("{}:4141", context.config.devnet.da_port),
            &image,
        ])
        .output()
        .await
        .map_err(|e| HylixError::process(format!("Failed to start Docker container: {}", e)))?;

    if !output.status.success() {
        log_warning(&format!(
            "Error: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    } else {
        pb.set_message("Hyli node started successfully");
    }

    // Wait for the node to be ready by checking block height
    pb.set_message("Waiting for node to be ready...");
    wait_for_block_height(pb, context, 2).await?;

    pb.set_message("Hyli node started successfully");
    Ok(())
}

/// Setup wallet app
async fn start_wallet_app(pb: &indicatif::ProgressBar, context: &DevnetContext) -> HylixResult<()> {
    use tokio::process::Command;

    let image = format!(
        "ghcr.io/hyli-org/wallet/wallet-server:{}",
        context.config.devnet.wallet_version
    );

    pull_docker_image(pb, &image).await?;

    pb.set_message("Starting wallet app...");

    let output = Command::new("docker")
        .args([
            "run",
            "-d",
            "--network=hyli-devnet",
            "--name",
            "hyli-devnet-wallet",
            "-e",
            "HYLI_NODE_URL=http://hyli-devnet-node:4321",
            "-e",
            "HYLI_INDEXER_URL=http://hyli-devnet-indexer:4321",
            "-e",
            "HYLI_DA_READ_FROM=hyli-devnet-node:4141",
            "-p",
            &format!("{}:4000", context.config.devnet.wallet_api_port),
            &image,
            "/app/server",
            "-m",
            "-w",
            "-a",
        ])
        .output()
        .await
        .map_err(|e| HylixError::process(format!("Failed to start Docker container: {}", e)))?;

    if !output.status.success() {
        log_warning(&format!(
            "Error: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    } else {
        pb.set_message("Hyli wallet app started successfully");
    }

    start_wallet_ui(pb, context).await
}

async fn start_wallet_ui(pb: &indicatif::ProgressBar, context: &DevnetContext) -> HylixResult<()> {
    use tokio::process::Command;

    let image = format!(
        "ghcr.io/hyli-org/wallet/wallet-ui:{}",
        context.config.devnet.wallet_version
    );

    pull_docker_image(pb, &image).await?;

    pb.set_message("Starting wallet UI...");

    let output = Command::new("docker")
        .args([
            "run",
            "-d",
            "--network=hyli-devnet",
            "--name",
            "hyli-devnet-wallet-ui",
            "-p",
            &format!("{}:80", context.config.devnet.wallet_ui_port),
            &image,
        ])
        .output()
        .await
        .map_err(|e| HylixError::process(format!("Failed to start Docker container: {}", e)))?;

    if !output.status.success() {
        log_warning(&format!(
            "Error: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    } else {
        pb.set_message("Hyli wallet UI started successfully");
    }

    Ok(())
}

/// Start the postgres server
async fn start_postgres_server(
    pb: &indicatif::ProgressBar,
    context: &DevnetContext,
) -> HylixResult<()> {
    use tokio::process::Command;

    pull_docker_image(pb, "postgres:17").await?;

    pb.set_message("Starting postgres server...");

    let output = Command::new("docker")
        .args([
            "run",
            "-d",
            "--network=hyli-devnet",
            "--name",
            "hyli-devnet-postgres",
            "-p",
            &format!("{}:5432", context.config.devnet.postgres_port),
            "-e",
            "POSTGRES_USER=postgres",
            "-e",
            "POSTGRES_PASSWORD=postgres",
            "-e",
            "POSTGRES_DB=hyli_indexer",
            "postgres:17",
        ])
        .output()
        .await
        .map_err(|e| HylixError::process(format!("Failed to start Docker container: {}", e)))?;

    if !output.status.success() {
        log_warning(&format!(
            "Error: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    } else {
        pb.set_message("Hyli postgres server started successfully");
    }

    wait_for_postgres_server(pb).await?;

    Ok(())
}

/// Start the indexer
async fn start_indexer(pb: &indicatif::ProgressBar, context: &DevnetContext) -> HylixResult<()> {
    use std::process::Command;
    let image = format!("ghcr.io/hyli-org/hyli:{}", context.config.devnet.version);

    start_postgres_server(pb, context).await?;

    pb.set_message("Starting Hyli indexer...");

    let output = Command::new("docker")
        .args([
            "run",
            "-d",
            "--network=hyli-devnet",
            "--name",
            "hyli-devnet-indexer",
            "-e",
            "HYLI_RUN_INDEXER=true",
            "-e",
            "HYLI_DATABASE_URL=postgresql://postgres:postgres@hyli-devnet-postgres:5432/hyli_indexer",
            "-e",
            "HYLI_DA_READ_FROM=hyli-devnet-node:4141",
            "-p",
            &format!("{}:4321", context.config.devnet.indexer_port),
            &image,
            "/hyli/indexer",
        ])
        .output()
        .map_err(|e| HylixError::process(format!("Failed to start Docker container: {}", e)))?;

    if !output.status.success() {
        log_warning(&format!(
            "Error: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    } else {
        pb.set_message("Hyli indexer started successfully");
    }

    Ok(())
}

/// Create pre-funded test accounts
async fn create_test_accounts(
    _pb: &indicatif::ProgressBar,
    _context: &DevnetContext,
) -> HylixResult<()> {
    // TODO: Implement test account creation
    // This would involve:
    // 1. Generating test account keys
    // 2. Funding them with test tokens
    // 3. Making them available for testing

    // Placeholder implementation
    std::thread::sleep(std::time::Duration::from_millis(500));

    Ok(())
}

/// Stop the wallet app
async fn stop_wallet_app(pb: &indicatif::ProgressBar) -> HylixResult<()> {
    // Stop and remove wallet app
    stop_and_remove_container(pb, "hyli-devnet-wallet", "Hyli wallet app").await?;

    // Stop and remove wallet UI
    stop_and_remove_container(pb, "hyli-devnet-wallet-ui", "Hyli wallet UI").await?;

    Ok(())
}

/// Stop the indexer
async fn stop_indexer(pb: &indicatif::ProgressBar) -> HylixResult<()> {
    // Stop and remove indexer
    stop_and_remove_container(pb, "hyli-devnet-indexer", "Hyli indexer").await?;

    // Stop and remove postgres server
    stop_and_remove_container(pb, "hyli-devnet-postgres", "Hyli postgres server").await?;

    Ok(())
}

/// Stop the local node
async fn stop_local_node(pb: &indicatif::ProgressBar) -> HylixResult<()> {
    stop_and_remove_container(pb, "hyli-devnet-node", "Hyli node").await
}

/// Remove the docker network
async fn remove_docker_network(pb: &indicatif::ProgressBar) -> HylixResult<()> {
    use tokio::process::Command;

    pb.set_message("Removing docker network...");
    let output = Command::new("docker")
        .args(["network", "rm", "hyli-devnet"])
        .output()
        .await
        .map_err(|e| HylixError::process(format!("Failed to remove Docker network: {}", e)))?;

    if !output.status.success() {
        log_warning(&format!("{}", String::from_utf8_lossy(&output.stderr)));
    } else {
        pb.set_message("Docker network removed successfully");
    }

    Ok(())
}

/// Wait for block height to reach a specific value
async fn wait_for_block_height(
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
                    "Timeout getting block height, retrying... (attempt {}/{})",
                    attempts, max_attempts
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
        "Failed to reach block height {} after {} attempts",
        target_height, max_attempts
    )))
}

/// Wait for the postgres server to be ready
async fn wait_for_postgres_server(pb: &indicatif::ProgressBar) -> HylixResult<()> {
    let mut attempts = 0;
    let max_attempts = 60;

    while attempts < max_attempts {
        pb.set_message("Waiting for postgres server to be ready...");
        let mut output = tokio::process::Command::new("docker")
            .args(["exec", "-it", "hyli-devnet-postgres", "pg_isready"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .map_err(|e| HylixError::process(format!("Failed to start Docker container: {}", e)))?;

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

/// Pull docker image
async fn pull_docker_image(pb: &indicatif::ProgressBar, image: &str) -> HylixResult<()> {
    use tokio::process::Command;

    pb.set_message(format!("Pulling docker image: {}", image));
    let output = Command::new("docker")
        .args(["pull", image])
        .output()
        .await
        .map_err(|e| HylixError::process(format!("Failed to pull Docker image: {}", e)))?;

    if !output.status.success() {
        log_warning("Failed to pull Docker image");
        if !output.stderr.is_empty() {
            log_warning(&format!(
                "Error: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        log_warning("Continuing with existing image if available");
    } else {
        pb.set_message("Docker image pulled successfully");
    }

    Ok(())
}

/// Stop and remove a Docker container
async fn stop_and_remove_container(
    pb: &indicatif::ProgressBar,
    container_name: &str,
    display_name: &str,
) -> HylixResult<()> {
    use tokio::process::Command;

    // Stop the container
    pb.set_message(format!("Stopping {}...", display_name));
    let output = Command::new("docker")
        .args(["stop", container_name])
        .output()
        .await
        .map_err(|e| HylixError::process(format!("Failed to stop Docker container: {}", e)))?;

    if !output.status.success() {
        log_warning(&format!("{}", String::from_utf8_lossy(&output.stderr)));
    } else {
        pb.set_message(format!("{} stopped successfully", display_name));
    }

    // Remove the container
    pb.set_message(format!("Removing {}...", display_name));
    let output = Command::new("docker")
        .args(["rm", container_name])
        .output()
        .await
        .map_err(|e| HylixError::process(format!("Failed to remove Docker container: {}", e)))?;

    if !output.status.success() {
        log_warning(&format!("{}", String::from_utf8_lossy(&output.stderr)));
    } else {
        pb.set_message(format!("{} removed successfully", display_name));
    }

    Ok(())
}

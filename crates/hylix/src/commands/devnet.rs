use crate::error::{HylixResult, HylixError};
use crate::logging::{create_progress_bar, log_success, log_info};
use crate::config::HylixConfig;
use client_sdk::rest_client::{NodeApiHttpClient, NodeApiClient};
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
    if reset {
        reset_devnet_state(context).await?;
    }

    // Start the local node
    start_local_node(context).await?;

    // Deploy Oranj token contract
    deploy_oranj_contract(context).await?;

    // Setup wallet app
    setup_wallet_app(context).await?;

    // Start indexer
    start_indexer(context).await?;

    // Start explorer
    start_explorer(context).await?;

    // Create pre-funded test accounts
    create_test_accounts(context).await?;

    log_success("Local devnet started successfully!");
    log_info("Services available at:");
    log_info(&format!("  Node: http://localhost:{}", context.config.devnet.node_port));
    log_info(&format!("  Explorer: http://localhost:{}", context.config.devnet.explorer_port));
    log_info(&format!("  Indexer: http://localhost:{}", context.config.devnet.indexer_port));

    Ok(())
}

/// Stop the local devnet
async fn stop_devnet(context: &DevnetContext) -> HylixResult<()> {
    let pb = create_progress_bar("Stopping local devnet...");

    // Stop explorer
    pb.set_message("Stopping explorer...");
    stop_explorer(&pb).await?;

    // Stop indexer
    pb.set_message("Stopping indexer...");
    stop_indexer(&pb).await?;

    // Stop local node
    pb.set_message("Stopping local node...");
    stop_local_node(&pb, context).await?;

    pb.finish_with_message("Local devnet stopped successfully!");
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
            Ok(_) => {
                Ok(true)
            }
            Err(e) => {
                Ok(false)
            }
        },
        Err(_) => {
            Ok(false)
        }
    }
}

/// Reset devnet state
async fn reset_devnet_state(context: &DevnetContext) -> HylixResult<()> {
    
    // TODO: Implement state reset
    // This would involve:
    // 1. Stopping all services
    // 2. Clearing the database
    // 3. Removing any cached data
    // 4. Starting fresh services
    
    Ok(())
}

/// Start the local node
async fn start_local_node(context: &DevnetContext) -> HylixResult<()> {
    use std::process::Command;
    
    let pb = create_progress_bar("Starting Hyli node with Docker...");
    
    let _output = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--network=host",
            "--name", "hyli-devnet",
            "-e", "HYLI_RUN_INDEXER=false",
            "-e", "HYLI_RUN_EXPLORER=false",
            "-p", &format!("{}:4321", context.config.devnet.node_port),
            "hyli"
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|e| HylixError::process(format!("Failed to start Docker container: {}", e)))?;
    
    // Wait for the node to be ready by checking block height
    pb.set_message("Waiting for node to be ready...");
    wait_for_block_height(&pb, context, 2).await?;
    
    pb.finish_with_message("Hyli node started successfully");
    Ok(())
}


/// Deploy Oranj token contract
async fn deploy_oranj_contract(context: &DevnetContext) -> HylixResult<()> {
    
    // TODO: Implement Oranj contract deployment
    // This would involve:
    // 1. Compiling the Oranj contract
    // 2. Deploying it to the local node
    // 3. Setting up auto-provers for the contract
    
    // Placeholder implementation
    std::thread::sleep(std::time::Duration::from_millis(500));
    
    Ok(())
}

/// Setup wallet app
async fn setup_wallet_app(context: &DevnetContext) -> HylixResult<()> {
    
    // TODO: Implement wallet app setup
    // This would involve:
    // 1. Deploying the wallet contract
    // 2. Setting up auto-provers
    // 3. Configuring the wallet interface
    
    // Placeholder implementation
    std::thread::sleep(std::time::Duration::from_millis(500));
    
    Ok(())
}

/// Start the indexer
async fn start_indexer(context: &DevnetContext) -> HylixResult<()> {
    
    // TODO: Implement indexer startup
    // This would involve:
    // 1. Starting the indexer process
    // 2. Configuring it to index the local node
    // 3. Setting up contract-specific indexing
    
    // Placeholder implementation
    std::thread::sleep(std::time::Duration::from_millis(500));
    
    Ok(())
}

/// Start the explorer
async fn start_explorer(context: &DevnetContext) -> HylixResult<()> {
    
    // TODO: Implement explorer startup
    // This would involve:
    // 1. Starting the explorer web application
    // 2. Configuring it to connect to the local node and indexer
    // 3. Setting up the web interface
    
    // Placeholder implementation
    std::thread::sleep(std::time::Duration::from_millis(500));
    
    Ok(())
}

/// Create pre-funded test accounts
async fn create_test_accounts(context: &DevnetContext) -> HylixResult<()> {
    
    // TODO: Implement test account creation
    // This would involve:
    // 1. Generating test account keys
    // 2. Funding them with test tokens
    // 3. Making them available for testing
    
    // Placeholder implementation
    std::thread::sleep(std::time::Duration::from_millis(500));
    
    Ok(())
}

/// Stop the explorer
async fn stop_explorer(pb: &indicatif::ProgressBar) -> HylixResult<()> {
    
    // TODO: Implement explorer shutdown
    // This would involve stopping the explorer process
    
    Ok(())
}

/// Stop the indexer
async fn stop_indexer(pb: &indicatif::ProgressBar) -> HylixResult<()> {
    
    // TODO: Implement indexer shutdown
    // This would involve stopping the indexer process
    
    Ok(())
}

/// Stop the local node
async fn stop_local_node(pb: &indicatif::ProgressBar, context: &DevnetContext) -> HylixResult<()> {
    use std::process::Command;

    Command::new("docker")
        .args(["stop", "hyli-devnet"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|e| HylixError::process(format!("Failed to stop Docker container: {}", e)))?;

    pb.set_message("Checking if node is stopped...");
    while is_devnet_running(context).await? {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    pb.set_message("Hyli node stopped successfully");

    Ok(())
}


/// Wait for block height to reach a specific value
async fn wait_for_block_height(pb: &indicatif::ProgressBar, context: &DevnetContext, target_height: u64) -> HylixResult<()> {
    let max_attempts = 60; // 1 minutes with 1-second intervals
    let mut attempts = 0;
    
    while attempts < max_attempts {
        match tokio::time::timeout(Duration::from_secs(5), context.client.get_block_height()).await {
            Ok(result) => match result {
                Ok(block_height) => {
                    let current_height = block_height.0;
                    
                    if current_height >= target_height {
                        pb.set_message("Hyli node started successfully");
                        return Ok(());
                    }
                }
                Err(e) => {
                    // Continue waiting
                }
            },
            Err(_) => {
                pb.set_message(format!("Timeout getting block height, retrying... (attempt {}/{})", attempts, max_attempts));
                // Continue waiting
            }
        }
        
        attempts += 1;
        if attempts < max_attempts {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    
    Err(HylixError::devnet(format!("Failed to reach block height {} after {} attempts", target_height, max_attempts)))
}

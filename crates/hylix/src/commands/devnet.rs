use crate::commands::bake::bake_devnet;
use crate::config::HylixConfig;
use crate::error::{HylixError, HylixResult};
use crate::logging::{
    create_progress_bar, create_progress_bar_with_msg, execute_command_with_progress, log_error,
    log_info, log_success, log_warning,
};
use client_sdk::rest_client::{NodeApiClient, NodeApiHttpClient};
use std::time::Duration;

/// Helper function to build Docker environment variable arguments
fn build_env_args(env_vars: &[String]) -> Vec<String> {
    let mut args = Vec::new();
    for env_var in env_vars {
        args.push("-e".to_string());
        args.push(env_var.clone());
    }
    args
}

/// Container status enum
#[derive(Debug, Clone, PartialEq)]
enum ContainerStatus {
    Running,
    Stopped,
    NotExisting,
}

/// Devnet action enum
#[derive(Debug, Clone)]
pub enum DevnetAction {
    Up {
        reset: bool,
        bake: bool,
        profile: Option<String>,
    },
    Down,
    Pause,
    Restart {
        reset: bool,
        bake: bool,
        profile: Option<String>,
    },
    Status,
    Fork {
        endpoint: String,
    },
    Bake {
        profile: Option<String>,
    },
    Env,
    Logs {
        service: String,
    },
}

/// Context struct containing client and config for devnet operations
pub struct DevnetContext {
    pub client: NodeApiHttpClient,
    pub config: HylixConfig,
    pub profile: Option<String>,
}

const CONTAINERS: [&str; 5] = [
    "hyli-devnet-node",
    "hyli-devnet-postgres",
    "hyli-devnet-indexer",
    "hyli-devnet-wallet",
    "hyli-devnet-wallet-ui",
];

impl DevnetContext {
    /// Create a new DevnetContext
    pub fn new(config: HylixConfig) -> HylixResult<Self> {
        let node_url = format!("http://localhost:{}", config.devnet.node_port);
        let client = NodeApiHttpClient::new(node_url)?;
        Ok(Self {
            client,
            config,
            profile: None,
        })
    }

    /// Create a new DevnetContext with a specific profile
    pub fn new_with_profile(config: HylixConfig, profile: Option<String>) -> HylixResult<Self> {
        let node_url = format!("http://localhost:{}", config.devnet.node_port);
        let client = NodeApiHttpClient::new(node_url)?;
        Ok(Self {
            client,
            config,
            profile,
        })
    }
}

/// Execute the `hy devnet` command
pub async fn execute(action: DevnetAction) -> HylixResult<()> {
    // Load configuration once
    let config = HylixConfig::load()?;
    let context = DevnetContext::new(config)?;

    match action {
        DevnetAction::Up {
            reset,
            bake,
            profile,
        } => {
            start_containers(&context).await?;

            if is_devnet_running(&context).await? {
                log_info("Devnet is running");
                return Ok(());
            }
            let context_with_profile = DevnetContext::new_with_profile(context.config, profile)?;
            start_devnet(reset, bake, &context_with_profile).await?;
        }
        DevnetAction::Pause => {
            pause_devnet(&context).await?;
        }
        DevnetAction::Down => {
            stop_devnet(&context).await?;
        }
        DevnetAction::Restart {
            reset,
            bake,
            profile,
        } => {
            let context_with_profile = DevnetContext::new_with_profile(context.config, profile)?;
            restart_devnet(reset, bake, &context_with_profile).await?;
        }
        DevnetAction::Status => {
            check_devnet_status(&context).await?;
        }
        DevnetAction::Fork { endpoint } => {
            fork_devnet(&endpoint).await?;
        }
        DevnetAction::Bake { profile } => {
            let context_with_profile = DevnetContext::new_with_profile(context.config, profile)?;
            bake_devnet(&indicatif::MultiProgress::new(), &context_with_profile).await?;
        }
        DevnetAction::Env => {
            print_devnet_env_vars(&context.config)?;
        }
        DevnetAction::Logs { service } => {
            logs_devnet(&service).await?;
        }
    }

    Ok(())
}

/// Logs the local devnet
async fn logs_devnet(service: &str) -> HylixResult<()> {
    let container = format!("hyli-devnet-{}", service);

    log_info(&format!("Following logs for {}", container));
    log_info("Use Ctrl+C to stop following logs");
    log_info(&format!(
        "{}",
        console::style(format!("$ docker logs -f {}", container)).green()
    ));

    use tokio::process::Command;

    loop {
        let mut status = get_docker_container_status(&container).await?;
        let pb = create_progress_bar();
        while status != ContainerStatus::Running {
            pb.set_message(format!("Waiting for container {} to be running", container));
            tokio::time::sleep(Duration::from_secs(1)).await;
            status = get_docker_container_status(&container).await?;
        }
        pb.finish_and_clear();

        Command::new("docker")
            .args(["logs", "-f", &container])
            .status()
            .await
            .map_err(|e| {
                HylixError::process(format!("Failed to get logs for {}: {}", container, e))
            })?;
    }
}

/// Check the status of the local devnet
async fn check_devnet_status(context: &DevnetContext) -> HylixResult<()> {
    let mut is_running = true;
    for container in CONTAINERS {
        let status = check_docker_container(context, container).await?;
        if status != ContainerStatus::Running {
            is_running = false;
            break;
        }
    }

    if is_running {
        log_success("Devnet is running");

        let block_height = context.client.get_block_height().await?;
        log_info(&format!("Block height: {}", block_height.0));

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
    } else {
        log_warning("Devnet is not running");
    }
    Ok(())
}

async fn check_docker_container(
    _context: &DevnetContext,
    container_name: &str,
) -> HylixResult<ContainerStatus> {
    let status = get_docker_container_status(container_name).await?;
    match status {
        ContainerStatus::Running => {
            log_success(&format!("Container {} is running", container_name));
        }
        ContainerStatus::Stopped => {
            log_warning(&format!("Container {} is stopped", container_name));
        }
        ContainerStatus::NotExisting => {
            log_error(&format!("Container {} does not exist", container_name));
        }
    }
    Ok(status)
}

async fn get_docker_container_status(container_name: &str) -> HylixResult<ContainerStatus> {
    use tokio::process::Command;

    // First check if container exists at all (running or stopped)
    let output = Command::new("docker")
        .args(["ps", "-a", "-q", "-f", &format!("name={}", container_name)])
        .output()
        .await
        .map_err(|e| HylixError::process(format!("Failed to check Docker container: {}", e)))?;

    if output.stdout.is_empty() {
        return Ok(ContainerStatus::NotExisting);
    }

    // If container exists, check if it's running
    let running_output = Command::new("docker")
        .args(["ps", "-q", "-f", &format!("name={}", container_name)])
        .output()
        .await
        .map_err(|e| HylixError::process(format!("Failed to check Docker container: {}", e)))?;

    if running_output.stdout.is_empty() {
        Ok(ContainerStatus::Stopped)
    } else {
        Ok(ContainerStatus::Running)
    }
}

async fn start_containers(context: &DevnetContext) -> HylixResult<()> {
    let mpb = indicatif::MultiProgress::new();
    for container in CONTAINERS {
        if get_docker_container_status(container).await? == ContainerStatus::Stopped {
            start_container(&mpb, context, container).await?;
        }
    }
    Ok(())
}

async fn start_container(
    mpb: &indicatif::MultiProgress,
    _context: &DevnetContext,
    container_name: &str,
) -> HylixResult<()> {
    let pb = mpb.add(create_progress_bar());
    pb.set_message(format!("Starting container {}", container_name));
    let success = execute_command_with_progress(
        mpb,
        "docker start",
        "docker",
        &["start", container_name],
        None,
    )
    .await?;
    pb.finish_and_clear();
    if success {
        log_success(&format!("Started container {}", container_name));
    } else {
        return Err(HylixError::process(format!(
            "Failed to start {}",
            container_name
        )));
    }
    Ok(())
}

/// Start the local devnet
async fn start_devnet(reset: bool, bake: bool, context: &DevnetContext) -> HylixResult<()> {
    // Check required dependencies before starting
    check_required_dependencies()?;

    let mpb = indicatif::MultiProgress::new();
    if reset {
        reset_devnet_state(context).await?;
    }

    create_docker_network(&mpb).await?;
    log_success("[1/4] Docker network created");

    // Start the local node
    start_local_node(&mpb, context).await?;
    log_success("[2/4] Local node started");

    // Start indexer
    start_indexer(&mpb, context).await?;
    log_success("[3/4] Indexer started");

    // Setup wallet app
    start_wallet_app(&mpb, context).await?;
    log_success("[4/4] Wallet app started");

    check_devnet_status(context).await?;

    if bake {
        bake_devnet(&mpb, context).await?;
    } else {
        log_warning("Skipping test account creation and funding");
        log_info(
            format!(
                "  Use {} to create and fund test accounts while starting devnet",
                console::style("--bake").green()
            )
            .as_str(),
        );
        log_info(
            format!(
                "  Run {} to create and fund test accounts later",
                console::style("hy devnet bake").green()
            )
            .as_str(),
        );
    }

    Ok(())
}

/// Pause the local devnet
async fn pause_devnet(_context: &DevnetContext) -> HylixResult<()> {
    let mpb = indicatif::MultiProgress::new();
    let pb = mpb.add(create_progress_bar());
    let pb2 = mpb.add(create_progress_bar());
    pb.set_message("Pausing local devnet...");
    for container in CONTAINERS {
        if get_docker_container_status(container).await? == ContainerStatus::Running {
            pb2.set_message(format!("Stopping container {}", container));
            execute_command_with_progress(
                &mpb,
                "docker stop",
                "docker",
                &["stop", container],
                None,
            )
            .await?;
        }
    }
    mpb.clear()
        .map_err(|e| HylixError::process(format!("Failed to clear progress bars: {}", e)))?;
    log_success("Local devnet paused successfully!");
    log_info("Use `hy devnet up` to resume the local devnet");
    Ok(())
}

/// Stop the local devnet
async fn stop_devnet(_context: &DevnetContext) -> HylixResult<()> {
    let pb = create_progress_bar_with_msg("Stopping local devnet...");

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
async fn restart_devnet(reset: bool, bake: bool, context: &DevnetContext) -> HylixResult<()> {
    stop_devnet(context).await?;
    start_devnet(reset, bake, context).await?;
    Ok(())
}

/// Fork a running network
async fn fork_devnet(_endpoint: &str) -> HylixResult<()> {
    // TODO: Implement network forking
    // This would involve:
    // 1. Connecting to the specified endpoint
    // 2. Downloading the current state
    // 3. Setting up a local node with that state
    // 4. Starting all the devnet services

    log_warning("Forking is not yet implemented.");
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
async fn create_docker_network(mpb: &indicatif::MultiProgress) -> HylixResult<()> {
    let pb = mpb.add(create_progress_bar());
    pb.set_message("Creating docker network...");

    let success = execute_command_with_progress(
        mpb,
        "docker network create",
        "docker",
        &["network", "create", "hyli-devnet"],
        None,
    )
    .await?;

    if !success {
        return Err(HylixError::process(
            "Failed to create Docker network".to_string(),
        ));
    }

    mpb.clear()
        .map_err(|e| HylixError::process(format!("Failed to clear progress bars: {}", e)))?;

    Ok(())
}

/// Start the local node
async fn start_local_node(
    mpb: &indicatif::MultiProgress,
    context: &DevnetContext,
) -> HylixResult<()> {
    let image = &context.config.devnet.node_image;

    pull_docker_image(mpb, image).await?;

    let pb = mpb.add(create_progress_bar());
    pb.set_message("Starting Hyli node with Docker...");

    // Build base arguments
    let mut args: Vec<String> = vec![
        "run",
        "-d",
        "--network",
        "hyli-devnet",
        "--name",
        "hyli-devnet-node",
        "-p",
        &format!("{}:4321", context.config.devnet.node_port),
        "-p",
        &format!("{}:4141", context.config.devnet.da_port),
        "-e",
        "RISC0_DEV_MODE=true",
        "-e",
        "SP1_PROVER=mock",
        "-e",
        "HYLI_RUN_INDEXER=false",
        "-e",
        "HYLI_RUN_EXPLORER=false",
    ]
    .into_iter()
    .map(String::from)
    .collect();

    // Add custom environment variables if configured
    args.extend(build_env_args(&context.config.devnet.container_env.node));

    args.push(image.to_string());

    let success = execute_command_with_progress(
        mpb,
        "docker run",
        "docker",
        &args.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
        None,
    )
    .await?;

    if success {
        pb.set_message("Hyli node started successfully");
    } else {
        return Err(HylixError::process(
            "Failed to start Docker container".to_string(),
        ));
    }

    // Wait for the node to be ready by checking block height
    pb.set_message("Waiting for node to be ready...");
    wait_for_block_height(&pb, context, 2).await?;

    mpb.clear()
        .map_err(|e| HylixError::process(format!("Failed to clear progress bars: {}", e)))?;

    Ok(())
}

/// Setup wallet app
async fn start_wallet_app(
    mpb: &indicatif::MultiProgress,
    context: &DevnetContext,
) -> HylixResult<()> {
    use tokio::process::Command;

    let image = &context.config.devnet.wallet_server_image;

    pull_docker_image(mpb, image).await?;

    let pb = mpb.add(create_progress_bar());
    pb.set_message("Starting wallet app...");

    let mut args: Vec<String> = [
        "run",
        "-d",
        "--network=hyli-devnet",
        "--name",
        "hyli-devnet-wallet",
        "-p",
        &format!("{}:4000", context.config.devnet.wallet_api_port),
        "-e",
        "RISC0_DEV_MODE=true",
        "-e",
        "HYLI_NODE_URL=http://hyli-devnet-node:4321",
        "-e",
        "HYLI_INDEXER_URL=http://hyli-devnet-indexer:4321",
        "-e",
        "HYLI_DA_READ_FROM=hyli-devnet-node:4141",
    ]
    .into_iter()
    .map(String::from)
    .collect();

    args.extend(build_env_args(
        &context.config.devnet.container_env.wallet_server,
    ));

    args.extend(vec![
        image.to_string(),
        "/app/server".to_string(),
        "-m".to_string(),
        "-w".to_string(),
        "-a".to_string(),
    ]);

    let output = Command::new("docker")
        .args(&args)
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

    start_wallet_ui(mpb, context).await
}

async fn start_wallet_ui(
    mpb: &indicatif::MultiProgress,
    context: &DevnetContext,
) -> HylixResult<()> {
    use tokio::process::Command;

    let image = &context.config.devnet.wallet_ui_image;

    pull_docker_image(mpb, image).await?;

    let pb = mpb.add(create_progress_bar());
    pb.set_message("Starting wallet UI...");

    let mut args: Vec<String> = [
        "run",
        "-d",
        "--network=hyli-devnet",
        "--name",
        "hyli-devnet-wallet-ui",
        "-e",
        &format!(
            "NODE_BASE_URL=http://localhost:{}",
            context.config.devnet.node_port
        ),
        "-e",
        &format!(
            "WALLET_SERVER_BASE_URL=http://localhost:{}",
            context.config.devnet.wallet_api_port
        ),
        "-e",
        &format!(
            "WALLET_WS_URL=ws://localhost:{}",
            context.config.devnet.wallet_ws_port
        ),
        "-e",
        &format!(
            "INDEXER_BASE_URL=http://localhost:{}",
            context.config.devnet.indexer_port
        ),
        "-e",
        "TX_EXPLORER_URL=https://explorer.hyli.org/",
        "-p",
        &format!("{}:80", context.config.devnet.wallet_ui_port),
    ]
    .into_iter()
    .map(String::from)
    .collect();

    args.extend(build_env_args(
        &context.config.devnet.container_env.wallet_ui,
    ));

    args.extend(vec![image.to_string()]);

    let output = Command::new("docker")
        .args(&args)
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

    mpb.clear()
        .map_err(|e| HylixError::process(format!("Failed to clear progress bars: {}", e)))?;

    Ok(())
}

/// Start the postgres server
async fn start_postgres_server(
    mpb: &indicatif::MultiProgress,
    context: &DevnetContext,
) -> HylixResult<()> {
    use tokio::process::Command;

    pull_docker_image(mpb, "postgres:17").await?;

    let pb = mpb.add(create_progress_bar());
    pb.set_message("Starting postgres server...");

    let mut args: Vec<String> = [
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
    ]
    .into_iter()
    .map(String::from)
    .collect();

    args.extend(build_env_args(
        &context.config.devnet.container_env.postgres,
    ));

    args.extend(vec!["postgres:17".to_string()]);

    let output = Command::new("docker")
        .args(&args)
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

    wait_for_postgres_server(&pb).await?;

    Ok(())
}

/// Start the indexer
async fn start_indexer(mpb: &indicatif::MultiProgress, context: &DevnetContext) -> HylixResult<()> {
    use std::process::Command;

    let image = &context.config.devnet.node_image;

    start_postgres_server(mpb, context).await?;

    let pb = mpb.add(create_progress_bar());
    pb.set_message("Starting Hyli indexer...");

    let mut args: Vec<String> = [
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
    ]
    .into_iter()
    .map(String::from)
    .collect();

    args.extend(build_env_args(&context.config.devnet.container_env.indexer));

    args.extend(vec![image.to_string(), "/hyli/indexer".to_string()]);

    let output = Command::new("docker")
        .args(&args)
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

    mpb.clear()
        .map_err(|e| HylixError::process(format!("Failed to clear progress bars: {}", e)))?;

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
async fn pull_docker_image(mpb: &indicatif::MultiProgress, image: &str) -> HylixResult<()> {
    let pb = mpb.add(create_progress_bar());
    pb.set_message(format!("Pulling docker image: {}", image));

    let success =
        execute_command_with_progress(mpb, "docker pull", "docker", &["pull", image], None).await?;

    if !success {
        return Err(HylixError::process(
            "Failed to pull Docker image".to_string(),
        ));
    }

    mpb.clear()
        .map_err(|e| HylixError::process(format!("Failed to clear progress bars: {}", e)))?;

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

/// Check required dependencies
fn check_required_dependencies() -> HylixResult<()> {
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
fn print_devnet_env_vars(config: &HylixConfig) -> HylixResult<()> {
    let devnet = &config.devnet;

    println!("# Hyli devnet environment variables");
    println!("# Source this file in your bash shell: source <(hy devnet env)");
    println!();

    // Node and DA endpoints
    println!(
        "export HYLI_NODE_URL=\"http://localhost:{}\"",
        devnet.node_port
    );
    println!("export HYLI_DA_READ_FROM=\"localhost:{}\"", devnet.da_port);

    // Indexer endpoint
    println!(
        "export HYLI_INDEXER_URL=\"http://localhost:{}\"",
        devnet.indexer_port
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
        "export HYLI_DATABASE_URL=\"postgresql://postgres:postgres@localhost:{}/hyli_indexer\"",
        devnet.postgres_port
    );

    // Explorer URL
    println!("export HYLI_EXPLORER_URL=\"https://explorer.hyli.org/?network=localhost&indexer={}&node={}&wallet={}\"", 
             devnet.indexer_port, devnet.node_port, devnet.wallet_api_port);

    // Development mode flags
    println!("export RISC0_DEV_MODE=\"1\"");
    println!("export SP1_PROVER=\"mock\"");

    println!();
    println!("# Usage examples:");
    println!("#   source <(hy devnet env)");
    println!("#   echo $HYLI_NODE_URL");
    println!("#   curl $HYLI_NODE_URL/swagger-ui");

    Ok(())
}

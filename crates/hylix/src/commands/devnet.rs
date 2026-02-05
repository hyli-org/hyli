use crate::commands::bake::bake_devnet;
use crate::config::HylixConfig;
use crate::constants;
use crate::docker::{ContainerManager, ContainerSpec};
use crate::env_builder::EnvBuilder;
use crate::error::{HylixError, HylixResult};
use crate::logging::{
    create_progress_bar, create_progress_bar_with_msg, log_error, log_info, log_success,
    log_warning, ProgressExecutor,
};
use client_sdk::rest_client::{NodeApiClient, NodeApiHttpClient};
use std::time::Duration;

/// Container status enum
#[derive(Debug, Clone, PartialEq)]
enum ContainerStatus {
    Running,
    Stopped,
    NotExisting,
}

/// Devnet action enum
#[derive(Debug, Clone, clap::Subcommand)]
pub enum DevnetAction {
    /// Start the local devnet
    #[command(alias = "u")]
    Up {
        /// Reset to fresh state
        #[arg(long)]
        reset: bool,
        /// Create and fund test accounts after starting devnet
        #[arg(long)]
        bake: bool,
        /// Profile to use for baking (e.g., --profile=bobalice)
        #[arg(long, value_name = "PROFILE")]
        profile: Option<String>,
        /// No pull docker images (use existing local)
        #[arg(long)]
        no_pull: bool,
    },
    /// Stop the local devnet
    #[command(alias = "d")]
    Down,
    /// Pause the local devnet
    #[command(alias = "p")]
    Pause,
    /// Check the status of the local devnet
    #[command(alias = "ps")]
    Status,
    /// Restart the local devnet
    #[command(alias = "r")]
    Restart {
        /// Reset to fresh state
        #[arg(long)]
        reset: bool,
        /// Create and fund test accounts after restarting devnet
        #[arg(long)]
        bake: bool,
        /// Profile to use for baking (e.g., --profile=bobalice)
        #[arg(long, value_name = "PROFILE")]
        profile: Option<String>,
        /// No pull docker images (use existing local)
        #[arg(long)]
        no_pull: bool,
    },
    /// Create and fund test accounts
    #[command(alias = "b")]
    Bake {
        /// Profile to use for baking
        profile: Option<String>,
    },
    /// Fork a running network
    #[command(alias = "f")]
    Fork {
        /// Network endpoint to fork
        endpoint: String,
    },
    /// Print environment variables for sourcing in bash
    #[command(alias = "e")]
    Env,
    /// Follow logs of a devnet service
    #[command(alias = "l")]
    Logs {
        /// Service to follow logs for
        service: String,
    },
}

/// Context struct containing client and config for devnet operations
pub struct DevnetContext {
    pub client: NodeApiHttpClient,
    pub config: HylixConfig,
    pub profile: Option<String>,
    pub pull: bool,
}

impl From<HylixConfig> for DevnetContext {
    fn from(config: HylixConfig) -> Self {
        DevnetContext::new(config).unwrap()
    }
}

const CONTAINERS: [&str; 7] = constants::containers::ALL;

impl DevnetContext {
    /// Create a new DevnetContext
    pub fn new(config: HylixConfig) -> HylixResult<Self> {
        let node_url = format!("http://localhost:{}", config.devnet.node_port);
        let client = NodeApiHttpClient::new(node_url)?;
        Ok(Self {
            client,
            config,
            profile: None,
            pull: true,
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
            pull: true,
        })
    }

    pub fn without_pull(&mut self) {
        self.pull = false;
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
            no_pull,
        } => {
            start_containers(&context).await?;

            if is_devnet_responding(&context).await? {
                log_info("Devnet is running");
                return Ok(());
            }
            let mut context_with_profile =
                DevnetContext::new_with_profile(context.config, profile)?;
            if no_pull {
                context_with_profile.without_pull();
            }
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
            no_pull,
        } => {
            let mut context_with_profile =
                DevnetContext::new_with_profile(context.config, profile)?;
            if no_pull {
                context_with_profile.without_pull();
            }
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
            let executor = ProgressExecutor::new();
            bake_devnet(&executor, &context_with_profile).await?;
            executor.clear()?;
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
    let container = format!("{}-{service}", constants::networks::DEVNET);

    log_info(&format!("Following logs for {container}"));
    log_info("Use Ctrl+C to stop following logs");
    log_info(&format!(
        "{}",
        console::style(format!("$ docker logs -f {container}")).green()
    ));

    use tokio::process::Command;

    loop {
        let mut status = get_docker_container_status(&container).await?;
        let pb = create_progress_bar();
        while status != ContainerStatus::Running {
            pb.set_message(format!("Waiting for container {container} to be running"));
            tokio::time::sleep(Duration::from_secs(1)).await;
            status = get_docker_container_status(&container).await?;
        }
        pb.finish_and_clear();

        Command::new("docker")
            .args(["logs", "-f", &container])
            .status()
            .await
            .map_err(|e| HylixError::process(format!("Failed to get logs for {container}: {e}")))?;
    }
}

/// Check the status of the local devnet
async fn check_devnet_status(context: &DevnetContext) -> HylixResult<()> {
    for container in CONTAINERS {
        check_docker_container(context, container).await?;
    }

    let is_running = check_docker_container(context, constants::containers::NODE).await?
        == ContainerStatus::Running
        && is_devnet_responding(context).await?;

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
            "  Registry API: http://localhost:{}/swagger-ui",
            context.config.devnet.registry_server_port
        ));
        log_info(&format!(
            "  Registry UI: http://localhost:{}",
            context.config.devnet.registry_ui_port
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
            log_success(&format!("Container {container_name} is running"));
        }
        ContainerStatus::Stopped => {
            log_warning(&format!("Container {container_name} is stopped"));
        }
        ContainerStatus::NotExisting => {
            log_error(&format!("Container {container_name} does not exist"));
        }
    }
    Ok(status)
}

async fn get_docker_container_status(container_name: &str) -> HylixResult<ContainerStatus> {
    use tokio::process::Command;

    // First check if container exists at all (running or stopped)
    let output = Command::new("docker")
        .args(["ps", "-a", "-q", "-f", &format!("name={container_name}")])
        .output()
        .await
        .map_err(|e| HylixError::process(format!("Failed to check Docker container: {e}")))?;

    if output.stdout.is_empty() {
        return Ok(ContainerStatus::NotExisting);
    }

    // If container exists, check if it's running
    let running_output = Command::new("docker")
        .args(["ps", "-q", "-f", &format!("name={container_name}")])
        .output()
        .await
        .map_err(|e| HylixError::process(format!("Failed to check Docker container: {e}")))?;

    if running_output.stdout.is_empty() {
        Ok(ContainerStatus::Stopped)
    } else {
        Ok(ContainerStatus::Running)
    }
}

async fn start_containers(context: &DevnetContext) -> HylixResult<()> {
    let executor = ProgressExecutor::new();
    for container in CONTAINERS {
        if get_docker_container_status(container).await? == ContainerStatus::Stopped {
            start_container(&executor, context, container).await?;
        }
    }
    executor.clear()?;
    Ok(())
}

async fn start_container(
    executor: &ProgressExecutor,
    _context: &DevnetContext,
    container_name: &str,
) -> HylixResult<()> {
    let success = executor
        .execute_command(
            format!("Starting container {container_name}"),
            "docker",
            &["start", container_name],
            None,
        )
        .await?;
    if success {
        log_success(&format!("Started container {container_name}"));
    } else {
        return Err(HylixError::process(format!(
            "Failed to start {container_name}"
        )));
    }
    Ok(())
}

/// Start the local devnet
async fn start_devnet(reset: bool, bake: bool, context: &DevnetContext) -> HylixResult<()> {
    // Check required dependencies before starting
    check_required_dependencies()?;

    let executor = ProgressExecutor::new();
    if reset {
        reset_devnet_state(context).await?;
    }

    create_docker_network(&executor).await?;
    log_success("[1/5] Docker network created");

    // Start the local node
    start_local_node(&executor, context).await?;
    log_success("[2/5] Local node started");

    // Start indexer
    start_indexer(&executor, context).await?;
    log_success("[3/5] Indexer started");

    // Start registry
    start_registry(&executor, context).await?;
    log_success("[4/5] Registry started");

    // Setup wallet app
    start_wallet_app(&executor, context).await?;
    log_success("[5/5] Wallet app started");

    check_devnet_status(context).await?;

    if bake {
        bake_devnet(&executor, context).await?;
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

    executor.clear()?;
    Ok(())
}

/// Pause the local devnet
async fn pause_devnet(_context: &DevnetContext) -> HylixResult<()> {
    let executor = ProgressExecutor::new();
    let _pb = executor.add_task("Pausing local devnet...");
    for container in CONTAINERS {
        if get_docker_container_status(container).await? == ContainerStatus::Running {
            executor
                .execute_command(
                    format!("Stopping container {container}"),
                    "docker",
                    &["stop", container],
                    None,
                )
                .await?;
        }
    }
    executor.clear()?;
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

    // Stop registry
    pb.set_message("Stopping registry...");
    stop_registry(&pb).await?;

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
async fn create_docker_network(executor: &ProgressExecutor) -> HylixResult<()> {
    let success = executor
        .execute_command(
            "Creating docker network...",
            "docker",
            &["network", "create", constants::networks::DEVNET],
            None,
        )
        .await?;

    if !success {
        return Err(HylixError::process(
            "Failed to create Docker network".to_string(),
        ));
    }

    executor.clear()?;

    Ok(())
}

/// Start the local node
async fn start_local_node(executor: &ProgressExecutor, context: &DevnetContext) -> HylixResult<()> {
    let image = &context.config.devnet.node_image;

    let env_builder = EnvBuilder::new()
        .risc0_dev_mode()
        .sp1_prover_mock()
        .set(constants::env_vars::HYLI_RUN_INDEXER, "false")
        .set(constants::env_vars::HYLI_RUN_EXPLORER, "false")
        .rust_log(&context.config.devnet.node_rust_log);

    let spec = ContainerSpec::new(constants::containers::NODE, image)
        .port(context.config.devnet.node_port, 4321)
        .port(context.config.devnet.da_port, 4141)
        .env_builder(env_builder)
        .custom_env(context.config.devnet.container_env.node.clone());

    ContainerManager::start_container(executor, spec, context.pull).await?;

    // Wait for the node to be ready by checking block height
    let pb = executor.add_task("Waiting for node to be ready...");
    wait_for_block_height(&pb, context, 2).await?;
    pb.finish_and_clear();

    Ok(())
}

/// Setup wallet app
async fn start_wallet_app(executor: &ProgressExecutor, context: &DevnetContext) -> HylixResult<()> {
    let image = &context.config.devnet.wallet_server_image;

    let env_builder = EnvBuilder::new()
        .set(
            constants::env_vars::RISC0_DEV_MODE,
            constants::env_values::RISC0_DEV_MODE_TRUE,
        )
        .set(
            constants::env_vars::HYLI_NODE_URL,
            &format!("http://{}:4321", constants::containers::NODE),
        )
        .set(
            constants::env_vars::HYLI_INDEXER_URL,
            &format!("http://{}:4321", constants::containers::INDEXER),
        )
        .set(
            constants::env_vars::HYLI_DA_READ_FROM,
            &format!("{}:4141", constants::containers::NODE),
        )
        .set(
            constants::env_vars::HYLI_REGISTRY_URL,
            &format!(
                "http://{}:{}",
                constants::containers::REGISTRY,
                context.config.devnet.registry_server_port
            ),
        )
        .set(
            constants::env_vars::HYLI_REGISTRY_API_KEY,
            constants::env_values::REGISTRY_API_KEY_DEV,
        );

    let spec = ContainerSpec::new(constants::containers::WALLET, image)
        .port(context.config.devnet.wallet_api_port, 4000)
        .env_builder(env_builder)
        .custom_env(context.config.devnet.container_env.wallet_server.clone())
        .args(vec![
            "/app/server".to_string(),
            "-m".to_string(),
            "-w".to_string(),
            "-a".to_string(),
        ]);

    ContainerManager::start_container(executor, spec, context.pull).await?;

    start_wallet_ui(executor, context).await
}

async fn start_wallet_ui(executor: &ProgressExecutor, context: &DevnetContext) -> HylixResult<()> {
    let image = &context.config.devnet.wallet_ui_image;

    let env_builder = EnvBuilder::new()
        .set(
            "NODE_BASE_URL",
            &format!("http://localhost:{}", context.config.devnet.node_port),
        )
        .set(
            "WALLET_SERVER_BASE_URL",
            &format!("http://localhost:{}", context.config.devnet.wallet_api_port),
        )
        .set(
            "WALLET_WS_URL",
            &format!("ws://localhost:{}", context.config.devnet.wallet_ws_port),
        )
        .set(
            "INDEXER_BASE_URL",
            &format!("http://localhost:{}", context.config.devnet.indexer_port),
        )
        .set("TX_EXPLORER_URL", "https://explorer.hyli.org/");

    let spec = ContainerSpec::new(constants::containers::WALLET_UI, image)
        .port(context.config.devnet.wallet_ui_port, 80)
        .env_builder(env_builder)
        .custom_env(context.config.devnet.container_env.wallet_ui.clone());

    ContainerManager::start_container(executor, spec, context.pull).await?;

    Ok(())
}

/// Start the registry app
async fn start_registry(executor: &ProgressExecutor, context: &DevnetContext) -> HylixResult<()> {
    let image = &context.config.devnet.registry_server_image;

    let env_builder = EnvBuilder::new().set(
        constants::env_vars::HYLI_REGISTRY_API_KEY,
        constants::env_values::REGISTRY_API_KEY_DEV,
    );

    let spec = ContainerSpec::new(constants::containers::REGISTRY, image)
        .port(context.config.devnet.registry_server_port, 9003)
        .env_builder(env_builder)
        .custom_env(context.config.devnet.container_env.registry_server.clone())
        .arg("/app/server".to_string());

    ContainerManager::start_container(executor, spec, context.pull).await?;

    start_registry_ui(executor, context).await
}

async fn start_registry_ui(
    executor: &ProgressExecutor,
    context: &DevnetContext,
) -> HylixResult<()> {
    let image = &context.config.devnet.registry_ui_image;

    let env_builder = EnvBuilder::new().set(
        "REGISTRY_SERVER_BASE_URL",
        &format!(
            "http://localhost:{}",
            context.config.devnet.registry_server_port
        ),
    );

    let spec = ContainerSpec::new(constants::containers::REGISTRY_UI, image)
        .port(context.config.devnet.registry_ui_port, 80)
        .env_builder(env_builder);

    ContainerManager::start_container(executor, spec, context.pull).await?;

    Ok(())
}

/// Start the postgres server
async fn start_postgres_server(
    executor: &ProgressExecutor,
    context: &DevnetContext,
) -> HylixResult<()> {
    let env_builder = EnvBuilder::new()
        .set("POSTGRES_USER", "postgres")
        .set("POSTGRES_PASSWORD", "postgres")
        .set("POSTGRES_DB", "hyli_indexer");

    let spec = ContainerSpec::new(constants::containers::POSTGRES, constants::images::POSTGRES)
        .port(context.config.devnet.postgres_port, 5432)
        .env_builder(env_builder)
        .custom_env(context.config.devnet.container_env.postgres.clone());

    ContainerManager::start_container(executor, spec, context.pull).await?;

    let pb = executor.add_task("Waiting for postgres server to be ready...");
    wait_for_postgres_server(&pb).await?;
    pb.finish_and_clear();

    Ok(())
}

/// Start the indexer
async fn start_indexer(executor: &ProgressExecutor, context: &DevnetContext) -> HylixResult<()> {
    let image = &context.config.devnet.node_image;

    start_postgres_server(executor, context).await?;

    let env_builder = EnvBuilder::new()
        .set(constants::env_vars::HYLI_RUN_INDEXER, "true")
        .set(
            constants::env_vars::HYLI_DATABASE_URL,
            &format!(
                "postgresql://postgres:postgres@{}:5432/hyli_indexer",
                constants::containers::POSTGRES
            ),
        )
        .set(
            constants::env_vars::HYLI_DA_READ_FROM,
            &format!("{}:4141", constants::containers::NODE),
        )
        .rust_log(&context.config.devnet.node_rust_log);

    let spec = ContainerSpec::new(constants::containers::INDEXER, image)
        .port(context.config.devnet.indexer_port, 4321)
        .env_builder(env_builder)
        .custom_env(context.config.devnet.container_env.indexer.clone())
        .arg("/hyli/indexer".to_string());

    ContainerManager::start_container(executor, spec, context.pull).await?;

    Ok(())
}

/// Stop the wallet app
async fn stop_wallet_app(pb: &indicatif::ProgressBar) -> HylixResult<()> {
    // Stop and remove wallet app
    stop_and_remove_container(pb, constants::containers::WALLET, "Hyli wallet app").await?;

    // Stop and remove wallet UI
    stop_and_remove_container(pb, constants::containers::WALLET_UI, "Hyli wallet UI").await?;

    Ok(())
}

/// Stop the registry
async fn stop_registry(pb: &indicatif::ProgressBar) -> HylixResult<()> {
    // Stop and remove registry server
    stop_and_remove_container(pb, constants::containers::REGISTRY, "Hyli registry server").await?;

    // Stop and remove registry UI
    stop_and_remove_container(pb, constants::containers::REGISTRY_UI, "Hyli registry UI").await?;

    Ok(())
}

/// Stop the indexer
async fn stop_indexer(pb: &indicatif::ProgressBar) -> HylixResult<()> {
    // Stop and remove indexer
    stop_and_remove_container(pb, constants::containers::INDEXER, "Hyli indexer").await?;

    // Stop and remove postgres server
    stop_and_remove_container(pb, constants::containers::POSTGRES, "Hyli postgres server").await?;

    Ok(())
}

/// Stop the local node
async fn stop_local_node(pb: &indicatif::ProgressBar) -> HylixResult<()> {
    stop_and_remove_container(pb, constants::containers::NODE, "Hyli node").await
}

/// Remove the docker network
async fn remove_docker_network(pb: &indicatif::ProgressBar) -> HylixResult<()> {
    use tokio::process::Command;

    pb.set_message("Removing docker network...");
    let output = Command::new("docker")
        .args(["network", "rm", constants::networks::DEVNET])
        .output()
        .await
        .map_err(|e| HylixError::process(format!("Failed to remove Docker network: {e}")))?;

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
async fn wait_for_postgres_server(pb: &indicatif::ProgressBar) -> HylixResult<()> {
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

/// Stop and remove a Docker container
async fn stop_and_remove_container(
    pb: &indicatif::ProgressBar,
    container_name: &str,
    display_name: &str,
) -> HylixResult<()> {
    use tokio::process::Command;

    // Stop the container
    pb.set_message(format!("Stopping {display_name}..."));
    let output = Command::new("docker")
        .args(["stop", container_name])
        .output()
        .await
        .map_err(|e| HylixError::process(format!("Failed to stop Docker container: {e}")))?;

    if !output.status.success() {
        log_warning(&format!("{}", String::from_utf8_lossy(&output.stderr)));
    } else {
        pb.set_message(format!("{display_name} stopped successfully"));
    }

    // Remove the container
    pb.set_message(format!("Removing {display_name}..."));
    let output = Command::new("docker")
        .args(["rm", container_name])
        .output()
        .await
        .map_err(|e| HylixError::process(format!("Failed to remove Docker container: {e}")))?;

    if !output.status.success() {
        log_warning(&format!("{}", String::from_utf8_lossy(&output.stderr)));
    } else {
        pb.set_message(format!("{display_name} removed successfully"));
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

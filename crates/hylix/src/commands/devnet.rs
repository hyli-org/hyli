use crate::commands::bake::bake_devnet;
use crate::config::HylixConfig;
use crate::docker::{ContainerManager, ContainerSpec};
use crate::env_builder::EnvBuilder;
use crate::error::{HylixError, HylixResult};
use crate::logging::{
    create_progress_bar, create_progress_bar_with_msg, log_error, log_info, log_success,
    log_warning, ProgressExecutor,
};
use crate::{constants, docker};
use client_sdk::rest_client::{NodeApiClient, NodeApiHttpClient};
use std::path::PathBuf;
use std::process::Command;
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
        /// Number of validator nodes for multi-node devnet (includes 1 local node by default)
        #[arg(long, value_name = "COUNT")]
        nodes: Option<u32>,
        /// Run all nodes in Docker (no local node for debugging)
        #[arg(long)]
        no_local: bool,
        /// Don't run extra services (indexer, registry, wallet)
        #[arg(long)]
        bare: bool,
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
        /// Don't run extra services (indexer, registry, wallet)
        #[arg(long)]
        bare: bool,
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
        /// Service to follow logs for (e.g., node, node-1, node-2, indexer)
        service: Option<String>,
        /// Follow logs of all nodes (multi-node only)
        #[arg(long)]
        all: bool,
    },
    /// Start the local node to join a multi-node devnet
    #[command(alias = "j")]
    Join {
        /// Only print the cargo command without executing
        #[arg(long)]
        dry_run: bool,
        /// Build in release mode
        #[arg(long)]
        release: bool,
        /// Additional arguments to pass to the hyli binary
        #[arg(last = true)]
        extra_args: Vec<String>,
    },
}

/// Multi-node configuration
#[derive(Debug, Clone)]
pub struct MultiNodeConfig {
    /// Total number of validators (including local node if enabled)
    pub total_nodes: u32,
    /// Number of Docker nodes
    pub docker_nodes: u32,
    /// Whether there's a local node for debugging
    pub has_local_node: bool,
    /// Genesis timestamp shared by all nodes
    pub genesis_timestamp: u64,
}

impl MultiNodeConfig {
    pub fn new(total_nodes: u32, no_local: bool) -> Self {
        let has_local_node = !no_local;
        let docker_nodes = if has_local_node {
            total_nodes - 1
        } else {
            total_nodes
        };
        let genesis_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        Self {
            total_nodes,
            docker_nodes,
            has_local_node,
            genesis_timestamp,
        }
    }
}

/// Context struct containing client and config for devnet operations
pub struct DevnetContext {
    pub client: NodeApiHttpClient,
    pub config: HylixConfig,
    pub profile: Option<String>,
    pub pull: bool,
    pub multi_node: Option<MultiNodeConfig>,
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
            multi_node: None,
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
            multi_node: None,
        })
    }

    pub fn without_pull(&mut self) {
        self.pull = false;
    }

    pub fn with_multi_node(&mut self, multi_node: MultiNodeConfig) {
        self.multi_node = Some(multi_node);
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
            nodes,
            no_local,
            bare,
        } => {
            // Multi-node mode
            if let Some(node_count) = nodes {
                if node_count < 2 {
                    return Err(HylixError::devnet(
                        "Multi-node devnet requires at least 2 nodes".to_string(),
                    ));
                }
                let multi_node = MultiNodeConfig::new(node_count, no_local);
                let mut context_with_profile =
                    DevnetContext::new_with_profile(context.config, profile)?;
                if no_pull {
                    context_with_profile.without_pull();
                }
                context_with_profile.with_multi_node(multi_node);
                start_multi_node_devnet(reset, bake, &context_with_profile, bare).await?;
                return Ok(());
            }

            // Single-node mode (existing behavior)
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
            start_devnet(reset, bake, &context_with_profile, bare).await?;
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
            bare,
        } => {
            let mut context_with_profile =
                DevnetContext::new_with_profile(context.config, profile)?;
            if no_pull {
                context_with_profile.without_pull();
            }
            restart_devnet(reset, bake, &context_with_profile, bare).await?;
        }
        DevnetAction::Status => {
            // Multi-node Devnet
            let node_1_status =
                get_docker_container_status(&constants::containers::node_name(1)).await?;
            if node_1_status != ContainerStatus::NotExisting {
                // if node-0 exists, then we have a local node
                let no_local = get_docker_container_status(&constants::containers::node_name(0))
                    .await?
                    != ContainerStatus::NotExisting;

                // get the number of nodes by checking existing containers
                let mut node_count = 1;
                while get_docker_container_status(&constants::containers::node_name(node_count))
                    .await?
                    != ContainerStatus::NotExisting
                {
                    node_count += 1;
                }

                log_info(&format!(
                    "Detected multi-node devnet with {} nodes",
                    node_count
                ));

                // Create a context with multi-node config
                let mut multi_node_context = context;
                let multi_node = MultiNodeConfig::new(node_count, no_local);
                multi_node_context.with_multi_node(multi_node);
                check_devnet_status(&multi_node_context).await?;
                return Ok(());
            }

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
        DevnetAction::Logs { service, all } => {
            if all {
                logs_all_nodes().await?;
            } else {
                let service_name = service.unwrap_or_else(|| "node".to_string());
                logs_devnet(&service_name).await?;
            }
        }
        DevnetAction::Join {
            dry_run,
            release,
            extra_args,
        } => {
            join_devnet(dry_run, release, extra_args).await?;
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
    let multi_node = context.multi_node.is_some();

    if multi_node {
        log_info("Checking status of multi-node devnet...");
    } else {
        log_info("Checking status of single-node devnet...");
    }

    let containers = if multi_node {
        let mut all_containers = Vec::new();
        let multi_node_config = context.multi_node.as_ref().unwrap();
        let start_index = if multi_node_config.has_local_node {
            1
        } else {
            0
        };
        for i in start_index..multi_node_config.total_nodes {
            all_containers.push(constants::containers::node_name(i));
        }
        all_containers.extend_from_slice(&[
            constants::containers::POSTGRES.to_string(),
            constants::containers::INDEXER.to_string(),
            constants::containers::REGISTRY.to_string(),
            constants::containers::REGISTRY_UI.to_string(),
            constants::containers::WALLET.to_string(),
            constants::containers::WALLET_UI.to_string(),
        ]);
        all_containers
    } else {
        CONTAINERS.to_vec().iter().map(|s| s.to_string()).collect()
    };

    for container in &containers {
        check_docker_container(context, container).await?;
    }

    let is_running = check_docker_container(context, &containers[0]).await?
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

    if multi_node {
        print_multi_node_status(context)?;
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
        .args(["ps", "-a", "-q", "-f", &format!("name=^{container_name}$")])
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
async fn start_devnet(
    reset: bool,
    bake: bool,
    context: &DevnetContext,
    bare: bool,
) -> HylixResult<()> {
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
    if bare {
        log_warning("[3/5] Skipping indexer in bare mode");
    } else {
        start_indexer(&executor, context).await?;
        log_success("[3/5] Indexer started");
    }

    // Start registry
    if bare {
        log_warning("[4/5] Skipping registry in bare mode");
    } else {
        start_registry(&executor, context).await?;
        log_success("[4/5] Registry started");
    }

    // Setup wallet app
    if bare {
        log_warning("[5/5] Skipping wallet app in bare mode");
    } else {
        start_wallet_app(&executor, context).await?;
        log_success("[5/5] Wallet app started");
    }

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
    let executor = ProgressExecutor::new();

    // Stop wallet app
    let pb = executor.add_task("Stopping wallet app...");
    stop_wallet_app(&pb).await?;
    drop(pb);
    log_success("Wallet app stopped");

    // Stop registry
    let pb = executor.add_task("Stopping registry...");
    stop_registry(&pb).await?;
    drop(pb);
    log_success("Registry stopped");

    // Stop indexer
    let pb = executor.add_task("Stopping indexer...");
    stop_indexer(&pb).await?;
    drop(pb);
    log_success("Indexer stopped");

    // Stop local node
    let pb = executor.add_task("Stopping local node...");
    stop_local_node(&pb).await?;
    drop(pb);
    log_success("Local node stopped");

    // Remove docker network
    let pb = executor.add_task("Removing docker network...");
    remove_docker_network(&pb).await?;
    drop(pb);
    log_success("Docker network removed");

    executor.clear()?;
    log_success("Local devnet stopped successfully!");
    Ok(())
}

/// Restart the local devnet
async fn restart_devnet(
    reset: bool,
    bake: bool,
    context: &DevnetContext,
    bare: bool,
) -> HylixResult<()> {
    stop_devnet(context).await?;
    start_devnet(reset, bake, context, bare).await?;
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
async fn create_docker_network(executor: &ProgressExecutor) -> HylixResult<String> {
    // Find an available subnet
    let subnet = docker::find_available_subnet().await?;

    let success = executor
        .execute_command(
            "Creating docker network...",
            "docker",
            &[
                "network",
                "create",
                "--subnet",
                &subnet,
                constants::networks::DEVNET,
            ],
            None,
        )
        .await?;

    if !success {
        return Err(HylixError::process(
            "Failed to create Docker network".to_string(),
        ));
    }

    executor.clear()?;

    Ok(subnet)
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

    let (node_name, ports) = if context.multi_node.is_some() {
        (
            constants::containers::node_name(1),
            NodePorts::for_docker_node(1, &context.config.devnet),
        )
    } else {
        (
            constants::containers::NODE.to_string(),
            NodePorts::for_docker_node(0, &context.config.devnet),
        )
    };

    let env_builder = EnvBuilder::new()
        .set(
            constants::env_vars::RISC0_DEV_MODE,
            constants::env_values::RISC0_DEV_MODE_TRUE,
        )
        .set(
            constants::env_vars::HYLI_NODE_URL,
            &format!("http://{node_name}:{}", ports.rest),
        )
        .set(
            constants::env_vars::HYLI_INDEXER_URL,
            &format!("http://{}:4321", constants::containers::INDEXER),
        )
        .set(
            constants::env_vars::HYLI_DA_READ_FROM,
            &format!("{node_name}:{}", ports.da),
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

    let (node_name, ports) = if context.multi_node.is_some() {
        (
            constants::containers::node_name(1),
            NodePorts::for_docker_node(1, &context.config.devnet),
        )
    } else {
        (
            constants::containers::NODE.to_string(),
            NodePorts::for_docker_node(0, &context.config.devnet),
        )
    };

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
            &format!("{node_name}:{}", ports.da),
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
    // Stop single-node container if it exists
    if get_docker_container_status(constants::containers::NODE).await?
        != ContainerStatus::NotExisting
    {
        log_info(&format!(
            "test: {:?}",
            get_docker_container_status(constants::containers::NODE).await?
        ));
        stop_and_remove_container(pb, constants::containers::NODE, "Hyli node").await?;
    }

    // Stop multi-node containers if they exist
    for i in 0..=100 {
        let name = constants::containers::node_name(i);
        if get_docker_container_status(&name).await? != ContainerStatus::NotExisting {
            stop_and_remove_container(pb, &name, &format!("node-{}", i)).await?;
        } else if i > 0 {
            break; // No more nodes
        }
    }

    Ok(())
}

/// Remove the docker network
async fn remove_docker_network(pb: &indicatif::ProgressBar) -> HylixResult<()> {
    use tokio::process::Command;

    // If network does not exist, skip
    let output = Command::new("docker")
        .args([
            "network",
            "ls",
            "-q",
            "-f",
            &format!("name=^{}", constants::networks::DEVNET),
        ])
        .output()
        .await
        .map_err(|e| HylixError::process(format!("Failed to check Docker networks: {e}")))?;
    if output.stdout.is_empty() {
        pb.set_message("Docker network does not exist, skipping...");
        return Ok(());
    }

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

    // If container does not exist, skip
    if get_docker_container_status(container_name).await? == ContainerStatus::NotExisting {
        pb.set_message(format!("{display_name} does not exist, skipping..."));
        return Ok(());
    }

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

// ============================================================================
// Multi-node devnet functions
// ============================================================================

/// Port configuration for a node in multi-node setup
#[derive(Debug, Clone)]
pub struct NodePorts {
    pub rest: u16,
    pub da: u16,
    pub p2p: u16,
    pub admin: u16,
}

impl NodePorts {
    fn for_docker_node(index: u32, base_config: &crate::config::DevnetConfig) -> Self {
        Self {
            rest: base_config.node_port + (index * 1000) as u16,
            da: base_config.da_port + (index * 1010) as u16,
            p2p: 1231 + (index * 1000) as u16,
            admin: 1111 + (index * 1111) as u16,
        }
    }

    fn for_local_node(base_config: &crate::config::DevnetConfig) -> Self {
        // Local node uses port 0 offsets (before Docker nodes)
        Self {
            rest: base_config.node_port, // 4321
            da: base_config.da_port,     // 4141
            p2p: 1231,
            admin: 1111,
        }
    }
}

/// Get the node ID for a given index
pub fn get_node_id(index: u32) -> String {
    format!("node-{}", index)
}

/// Generate stakers configuration for genesis
fn generate_stakers_config(total_nodes: u32, has_local_node: bool) -> String {
    let mut stakers = Vec::new();

    // Add Docker nodes
    let docker_start = if has_local_node { 1 } else { 0 };
    for i in docker_start..total_nodes {
        let node_id = if has_local_node && i == 0 {
            constants::containers::NODE_LOCAL.to_string()
        } else {
            get_node_id(i)
        };
        stakers.push(format!("\"{}\"=100", node_id));
    }

    // Add local node if enabled
    if has_local_node {
        stakers.insert(0, format!("\"{}\"=100", constants::containers::NODE_LOCAL));
    }

    stakers.join(",")
}

/// Generate the list of peer addresses for a node
fn generate_peers_for_node(
    node_index: u32,
    total_nodes: u32,
    has_local_node: bool,
    base_config: &crate::config::DevnetConfig,
) -> String {
    let mut peers = Vec::new();

    // Add all other nodes as peers
    for i in 0..total_nodes {
        if i == node_index {
            continue; // Skip self
        }

        let ports = if has_local_node && i == 0 {
            NodePorts::for_local_node(base_config)
        } else {
            NodePorts::for_docker_node(i, base_config)
        };

        let node_name = if has_local_node && i == 0 {
            constants::containers::DOCKER_HOST_NAME.to_string()
        } else if has_local_node && node_index == 0 {
            // Local node connecting to Docker exposed ports
            "127.0.0.1".to_string()
        } else {
            // Docker node connecting to other Docker nodes
            constants::containers::node_name(i)
        };

        peers.push(format!("{node_name}:{}", ports.p2p));
    }

    peers.join(",")
}

/// Start multi-node devnet
async fn start_multi_node_devnet(
    reset: bool,
    bake: bool,
    context: &DevnetContext,
    bare: bool,
) -> HylixResult<()> {
    let multi_node = context
        .multi_node
        .as_ref()
        .ok_or_else(|| HylixError::devnet("Multi-node configuration not set"))?;

    log_info(&format!(
        "Starting multi-node devnet with {} validators...",
        multi_node.total_nodes
    ));

    if multi_node.has_local_node {
        log_info(&format!(
            "  {} Docker nodes + 1 local node (node-local)",
            multi_node.docker_nodes
        ));
    } else {
        log_info(&format!("  {} Docker nodes", multi_node.docker_nodes));
    }

    // Check required dependencies
    check_required_dependencies()?;

    let executor = ProgressExecutor::new();

    if reset {
        reset_multi_node_state(context).await?;
    }

    // Create Docker network
    let subnet = create_docker_network(&executor).await?;
    log_success("[1/6] Docker network created");

    // Start all Docker nodes
    start_multi_node_nodes(&executor, context, subnet).await?;
    log_success(&format!(
        "[2/6] Started {} Docker nodes",
        multi_node.docker_nodes
    ));

    // Generate local node configuration if needed
    if multi_node.has_local_node {
        generate_local_node_config(context)?;
        log_success("[3/6] Local node configuration generated");
    } else {
        log_info("[3/6] Skipped local node configuration (--no-local)");
    }

    // Start indexer (connected to first Docker node)
    if bare {
        log_warning("[4/6] Skipping indexer in bare mode");
    } else {
        start_indexer(&executor, context).await?;
        log_success("[4/6] Indexer started");
    }

    // Start registry
    if bare {
        log_warning("[5/6] Skipping registry in bare mode");
    } else {
        start_registry(&executor, context).await?;
        log_success("[5/6] Registry started");
    }

    // Start wallet app
    if bare {
        log_warning("[5/6] Skipping wallet app in bare mode");
    } else {
        start_wallet_app(&executor, context).await?;
        log_success("[5/6] Wallet started");
    }

    // Print status
    check_devnet_status(context).await?;

    if multi_node.has_local_node {
        println!();
        log_info("Local node ready to join. Start it with:");
        println!();
        println!("   {}", console::style("hy devnet join").green().bold());
        println!();
        log_info("Or with cargo for debugging:");
        println!();
        let config_path = get_local_node_config_path()?;
        println!(
            "   {}",
            console::style(format!(
                "cargo run -p hyli -- --config-file {}",
                config_path.display()
            ))
            .green()
        );
        println!();
        log_warning("Consensus will start once the local node connects.");
    }

    if bake {
        // Wait for consensus to start before baking
        if multi_node.has_local_node {
            log_info("Waiting for local node to join before baking...");
            let pb = executor.add_task("Waiting for consensus to start...");
            wait_for_block_height(&pb, context, 2).await?;
            pb.finish_and_clear();
        }
        bake_devnet(&executor, context).await?;
    }

    executor.clear()?;
    log_success("Multi-node devnet is up!");

    Ok(())
}

/// Start Docker nodes for multi-node setup
async fn start_multi_node_nodes(
    executor: &ProgressExecutor,
    context: &DevnetContext,
    subnet: String,
) -> HylixResult<()> {
    let multi_node = context.multi_node.as_ref().unwrap();
    let image = &context.config.devnet.node_image;

    for i in 0..multi_node.docker_nodes {
        let node_index = if multi_node.has_local_node { i + 1 } else { i };
        let node_id = get_node_id(node_index);
        let container_name = constants::containers::node_name(node_index);
        let ports = NodePorts::for_docker_node(node_index, &context.config.devnet);

        let peers = generate_peers_for_node(
            node_index,
            multi_node.total_nodes,
            multi_node.has_local_node,
            &context.config.devnet,
        );

        let ip = format!(
            "{}.{}",
            subnet.split('.').take(3).collect::<Vec<&str>>().join("."),
            10 + node_index
        );

        let env_builder = EnvBuilder::new()
            .risc0_dev_mode()
            .sp1_prover_mock()
            .with_ports(&ip, &ports)
            .set(constants::env_vars::HYLI_RUN_INDEXER, "false")
            .set(constants::env_vars::HYLI_RUN_EXPLORER, "false")
            .set(constants::env_vars::HYLI_ID, &node_id)
            .set(constants::env_vars::HYLI_P2P__PEERS, &peers)
            .set(constants::env_vars::HYLI_CONSENSUS__SOLO, "false")
            .set(constants::env_vars::HYLI_CONSENSUS__SLOT_DURATION, "1000")
            .genesis_stakers(multi_node.total_nodes, multi_node.has_local_node)
            .rust_log(&context.config.devnet.node_rust_log);

        let spec = ContainerSpec::new(&container_name, image)
            .ip(&ip)
            .port(ports.rest, ports.rest)
            .port(ports.p2p, ports.p2p)
            .port(ports.da, ports.da)
            .port(ports.admin, ports.admin)
            .env_builder(env_builder)
            .custom_env(context.config.devnet.container_env.node.clone());

        ContainerManager::start_container(executor, spec, context.pull).await?;

        log_info(&format!(
            "  Started {} at {} (REST: {}, P2P: {}, DA: {}, Admin: {})",
            container_name, ip, ports.rest, ports.p2p, ports.da, ports.admin
        ));
    }

    // Wait for first Docker node to be ready
    if multi_node.docker_nodes > 0 && !multi_node.has_local_node {
        let pb = executor.add_task("Waiting for nodes to be ready...");
        wait_for_block_height(&pb, context, 2).await?;
        pb.finish_and_clear();
    }

    Ok(())
}

/// Generate configuration file for the local node
fn generate_local_node_config(context: &DevnetContext) -> HylixResult<()> {
    let multi_node = context.multi_node.as_ref().unwrap();
    let config_dir = get_local_node_config_dir()?;
    std::fs::create_dir_all(&config_dir)?;

    let config_path = config_dir.join("config.toml");
    let ports = NodePorts::for_local_node(&context.config.devnet);

    let peers = generate_peers_for_node(
        0, // Local node is index 0
        multi_node.total_nodes,
        true,
        &context.config.devnet,
    );

    let stakers = generate_stakers_config(multi_node.total_nodes, true);

    let host_ip = Command::new("docker")
        .args([
            "inspect",
            "-f",
            "{{(index .IPAM.Config 0).Gateway}}",
            constants::networks::DEVNET,
        ])
        .output()
        .map_err(|e| HylixError::process(format!("Failed to get Docker host IP: {e}")))?
        .stdout;
    let host_ip = String::from_utf8_lossy(&host_ip).trim().to_string();

    // Generate TOML configuration
    let config_content = format!(
        r#"# Auto-generated configuration for local node in multi-node devnet
# Generated by: hy devnet up --nodes {}

id = "{}"
data_directory = "data_node_local"

# REST API
run_rest_server = true
rest_server_port = {}

# DA Server
da_server_port = {}
da_public_address = "{host_ip}:{}"

# Indexer (disabled - using shared indexer)
run_indexer = false
run_explorer = false

[p2p]
mode = "FullValidator"
public_address = "{host_ip}:{}"
server_port = {}
peers = [{}]

[consensus]
solo = false
slot_duration = 1000
genesis_timestamp = {}

[genesis]
stakers = {{ {} }}
"#,
        multi_node.total_nodes,
        constants::containers::NODE_LOCAL,
        ports.rest,
        ports.da,
        ports.da,
        ports.p2p,
        ports.p2p,
        peers
            .split(',')
            .map(|p| format!("\"{}\"", p))
            .collect::<Vec<_>>()
            .join(", "),
        multi_node.genesis_timestamp,
        stakers.replace(',', ", ").replace('=', " = "),
    );

    std::fs::write(&config_path, config_content)?;

    log_info(&format!(
        "Local node configuration written to: {}",
        config_path.display()
    ));

    Ok(())
}

/// Get the directory for local node configuration
fn get_local_node_config_dir() -> HylixResult<PathBuf> {
    let config_dir =
        dirs::config_dir().ok_or_else(|| HylixError::config("Could not find config directory"))?;
    Ok(config_dir.join("hylix").join("devnet").join("node-local"))
}

/// Get the path to the local node configuration file
fn get_local_node_config_path() -> HylixResult<PathBuf> {
    Ok(get_local_node_config_dir()?.join("config.toml"))
}

/// Join the multi-node devnet with the local node
async fn join_devnet(dry_run: bool, release: bool, extra_args: Vec<String>) -> HylixResult<()> {
    let config_path = get_local_node_config_path()?;

    if !config_path.exists() {
        return Err(HylixError::devnet(format!(
            "Local node configuration not found at {}.\n\
             Please start a multi-node devnet first with: hy devnet up --nodes <N>",
            config_path.display()
        )));
    }

    let mut args = vec!["run", "-p", "hyli"];

    if release {
        args.push("--release");
    }

    args.push("--");
    args.push("--config-file");
    let config_path_str = config_path.to_string_lossy().to_string();

    // Build the command string for display
    let mut display_args = args.clone();
    display_args.push(&config_path_str);
    for arg in &extra_args {
        display_args.push(arg);
    }

    log_info(&format!(
        "{}",
        console::style(format!("$ cargo {}", display_args.join(" "))).green()
    ));

    if dry_run {
        println!();
        log_info("Run this command to start the local node.");
        return Ok(());
    }

    let config = HylixConfig::load()?;
    let env_builder = EnvBuilder::new()
        .risc0_dev_mode()
        .sp1_prover_mock()
        .rust_log(&config.devnet.node_rust_log);

    args.push(&config_path_str);
    for arg in &extra_args {
        args.push(arg);
    }

    let mut backend = env_builder
        .into_tokio_command("cargo")
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .args(&args)
        .spawn()
        .map_err(|e| HylixError::backend(format!("Failed to start local node: {e}")))?;

    log_success("Local node started!");
    log_info("Press Ctrl+C to stop.");

    match backend.wait().await {
        Ok(status) => {
            if status.success() {
                log_info("Local node stopped gracefully");
            } else {
                log_error(&format!("Local node exited with error: {status}"));
            }
        }
        Err(e) => {
            return Err(HylixError::backend(format!(
                "Error waiting for local node: {e}"
            )));
        }
    }

    Ok(())
}

/// Follow logs of all nodes
async fn logs_all_nodes() -> HylixResult<()> {
    // Check if we're in multi-node mode by looking for node-1 container
    let node1_status = get_docker_container_status(&constants::containers::node_name(1)).await?;

    if node1_status == ContainerStatus::NotExisting {
        return Err(HylixError::devnet(
            "No multi-node devnet running. Use 'hy devnet logs <service>' for single-node.",
        ));
    }

    log_info("Following logs of all Docker nodes...");
    log_info("Use Ctrl+C to stop");

    // Find all running node containers
    let mut containers = Vec::new();
    for i in 1..=10 {
        // Check up to 10 nodes
        let name = constants::containers::node_name(i);
        let status = get_docker_container_status(&name).await?;
        if status == ContainerStatus::Running {
            containers.push(name);
        } else {
            break;
        }
    }

    if containers.is_empty() {
        return Err(HylixError::devnet("No node containers are running"));
    }

    // Use docker logs with --follow for all containers
    // We'll use a simple approach: follow the first container
    // For a more sophisticated approach, we'd need to multiplex the logs
    log_info(&format!("Following logs of: {}", containers.join(", ")));
    log_info("Tip: Use 'hy devnet logs node-N' to follow a specific node");

    // Follow the first node's logs
    let container = &containers[0];
    log_info(&format!(
        "{}",
        console::style(format!("$ docker logs -f {container}")).green()
    ));

    tokio::process::Command::new("docker")
        .args(["logs", "-f", container])
        .status()
        .await
        .map_err(|e| HylixError::process(format!("Failed to get logs: {e}")))?;

    Ok(())
}

/// Reset multi-node devnet state
async fn reset_multi_node_state(context: &DevnetContext) -> HylixResult<()> {
    let pb = create_progress_bar_with_msg("Resetting multi-node devnet state...");

    // Stop and remove all node containers
    for i in 1..=10 {
        let name = constants::containers::node_name(i);
        if get_docker_container_status(&name).await? != ContainerStatus::NotExisting {
            stop_and_remove_container(&pb, &name, &format!("node-{}", i)).await?;
        } else {
            break;
        }
    }

    // Also clean single-node container if it exists
    if get_docker_container_status(constants::containers::NODE).await?
        != ContainerStatus::NotExisting
    {
        stop_and_remove_container(&pb, constants::containers::NODE, "single node").await?;
    }

    // Clean local node config
    let config_dir = get_local_node_config_dir()?;
    if config_dir.exists() {
        std::fs::remove_dir_all(&config_dir)?;
        log_info("Removed local node configuration");
    }

    pb.finish_and_clear();

    // Also reset indexer, wallet, etc.
    reset_devnet_state(context).await?;

    Ok(())
}

/// Print multi-node devnet status
fn print_multi_node_status(context: &DevnetContext) -> HylixResult<()> {
    let multi_node = context.multi_node.as_ref().unwrap();

    log_info("Nodes:");
    for i in 0..multi_node.docker_nodes {
        let node_index = if multi_node.has_local_node { i + 1 } else { i };
        let ports = NodePorts::for_docker_node(node_index, &context.config.devnet);
        println!(
            "  ● node-{}       docker   REST: {}   P2P: {}",
            node_index, ports.rest, ports.p2p
        );
    }

    if multi_node.has_local_node {
        let ports = NodePorts::for_local_node(&context.config.devnet);
        println!(
            "  ○ node-local   local    REST: {}   P2P: {}",
            ports.rest, ports.p2p
        );
    }

    Ok(())
}

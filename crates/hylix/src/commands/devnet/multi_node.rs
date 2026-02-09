use std::path::PathBuf;

use crate::commands::bake::bake_devnet;
use crate::constants;
use crate::docker::ContainerSpec;
use crate::env_builder::EnvBuilder;
use crate::error::{HylixError, HylixResult};
use crate::logging::{
    ProgressExecutor, create_progress_bar_with_msg, log_info, log_success, log_warning,
};

use super::config::{
    generate_local_node_config, generate_peers_for_node, get_local_node_config_dir,
    get_local_node_config_path, get_node_id,
};
use super::containers::{
    ContainerStatus, get_docker_container_status, start_managed_container,
    stop_and_remove_container,
};
use super::context::{DevnetContext, NodePorts};
use super::network::create_docker_network;
use super::services::{start_indexer, start_registry, start_wallet_app};
use super::single_node::reset_devnet_state;
use super::status::check_devnet_status;
use super::utils::{check_required_dependencies, wait_for_block_height};

/// Start multi-node devnet
pub async fn start_multi_node_devnet(
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

        start_managed_container(executor, spec, context.pull).await?;

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

/// Join the multi-node devnet with the local node
pub async fn join_devnet(
    dry_run: bool,
    resume: bool,
    release: bool,
    extra_args: Vec<String>,
) -> HylixResult<()> {
    if !resume {
        // don't join if local data already exists to avoid accidentally wiping state
        let data_dir = PathBuf::from("data_node_local");
        if data_dir.exists() {
            log_warning(
                "Existing local node data directory found at 'data_node_local'. Either remove it or use '--resume' to keep existing state.",
            );

            log_info(&format!(
                "{}",
                console::style("$ hy devnet join --resume").green()
            ));
            return Err(HylixError::devnet(
                "Local node data directory already exists.",
            ));
        }
    }

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

    let config = crate::config::HylixConfig::load()?;
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
                crate::logging::log_error(&format!("Local node exited with error: {status}"));
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

/// Reset multi-node devnet state
pub async fn reset_multi_node_state(context: &DevnetContext) -> HylixResult<()> {
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

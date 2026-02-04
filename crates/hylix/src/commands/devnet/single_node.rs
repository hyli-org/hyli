use crate::commands::bake::bake_devnet;
use crate::constants;
use crate::error::HylixResult;
use crate::logging::{log_info, log_success, log_warning, ProgressExecutor};

use super::context::DevnetContext;
use super::network::{create_docker_network, remove_docker_network};
use super::services::{start_indexer, start_local_node, start_registry, start_wallet_app};
use super::services::{stop_indexer, stop_registry, stop_wallet_app};
use super::status::check_devnet_status;
use super::utils::check_required_dependencies;
use super::containers::{get_docker_container_status, stop_and_remove_container, ContainerStatus};

/// Start the local devnet
pub async fn start_devnet(
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
pub async fn pause_devnet(_context: &DevnetContext) -> HylixResult<()> {
    const CONTAINERS: [&str; 7] = constants::containers::ALL;
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
pub async fn stop_devnet(_context: &DevnetContext) -> HylixResult<()> {
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
pub async fn restart_devnet(
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
pub async fn fork_devnet(_endpoint: &str) -> HylixResult<()> {
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

/// Reset devnet state
pub async fn reset_devnet_state(_context: &DevnetContext) -> HylixResult<()> {
    // TODO: Implement state reset
    // This would involve:
    // 1. Stopping all services
    // 2. Clearing the database
    // 3. Removing any cached data
    // 4. Starting fresh services

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

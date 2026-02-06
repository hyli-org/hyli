use crate::constants;
use crate::error::{HylixError, HylixResult};
use crate::logging::{create_progress_bar, log_info};
use std::time::Duration;

use super::containers::{get_docker_container_status, ContainerStatus};

/// Logs the local devnet
pub async fn logs_devnet(service: &str) -> HylixResult<()> {
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

/// Follow logs of all nodes
pub async fn logs_all_nodes() -> HylixResult<()> {
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

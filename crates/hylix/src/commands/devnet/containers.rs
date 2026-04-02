use crate::error::{HylixError, HylixResult};
use crate::logging::{log_error, log_success, log_warning, ProgressExecutor};
use crate::{constants, docker::ContainerSpec};

use super::context::DevnetContext;

/// Container status enum
#[derive(Debug, Clone, PartialEq)]
pub enum ContainerStatus {
    Running,
    Stopped,
    NotExisting,
}

pub async fn get_docker_container_status(container_name: &str) -> HylixResult<ContainerStatus> {
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

pub async fn check_docker_container(
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

pub async fn start_containers(context: &DevnetContext) -> HylixResult<()> {
    const CONTAINERS: [&str; 7] = constants::containers::ALL;
    let executor = ProgressExecutor::new();
    for container in CONTAINERS {
        if get_docker_container_status(container).await? == ContainerStatus::Stopped {
            start_container(&executor, context, container).await?;
        }
    }
    executor.clear()?;
    Ok(())
}

pub async fn start_container(
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

/// Stop and remove a Docker container
pub async fn stop_and_remove_container(
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

/// Start a container using ContainerManager
pub async fn start_managed_container(
    executor: &ProgressExecutor,
    spec: ContainerSpec,
    pull: bool,
) -> HylixResult<()> {
    crate::docker::ContainerManager::start_container(executor, spec, pull).await
}

use crate::constants;
use crate::error::{HylixError, HylixResult};
use crate::logging::{ProgressExecutor, log_warning};

/// Create the docker network
pub async fn create_docker_network(executor: &ProgressExecutor) -> HylixResult<String> {
    // Find an available subnet
    let subnet = crate::docker::find_available_subnet().await?;

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

/// Remove the docker network
pub async fn remove_docker_network(pb: &indicatif::ProgressBar) -> HylixResult<()> {
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

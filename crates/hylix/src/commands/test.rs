use crate::error::{HylixError, HylixResult};
use crate::logging::{create_progress_bar, log_success, log_info, log_error};
use std::process::Command;

/// Execute the `hy test` command
pub async fn execute(keep_alive: bool) -> HylixResult<()> {
    log_info("Running end-to-end tests...");

    // Check if we're in a valid project directory
    validate_project_directory()?;

    // Start devnet if not already running
    let pb = create_progress_bar("Starting devnet...");
    start_devnet_if_needed().await?;
    pb.finish_with_message("Devnet ready");

    // Build the project
    let pb = create_progress_bar("Building project...");
    build_project().await?;
    pb.finish_with_message("Project built");

    // Start backend
    let pb = create_progress_bar("Starting backend...");
    let backend_handle = start_backend().await?;
    pb.finish_with_message("Backend started");

    // Run tests
    let pb = create_progress_bar("Running tests...");
    run_tests().await?;
    pb.finish_with_message("Tests completed");

    // Cleanup
    if !keep_alive {
        let pb = create_progress_bar("Cleaning up...");
        cleanup(backend_handle).await?;
        pb.finish_with_message("Cleanup completed");
    } else {
        log_info("Keeping devnet and backend alive as requested");
    }

    log_success("All tests completed successfully!");
    Ok(())
}

/// Validate that we're in a valid project directory
fn validate_project_directory() -> HylixResult<()> {
    if !std::path::Path::new("contracts").exists() {
        return Err(HylixError::project(
            "No 'contracts' directory found. Are you in a Hylix project directory?"
        ));
    }

    if !std::path::Path::new("server").exists() {
        return Err(HylixError::project(
            "No 'server' directory found. Are you in a Hylix project directory?"
        ));
    }

    if !std::path::Path::new("tests").exists() {
        log_info("No 'tests' directory found, running unit tests only");
    }

    Ok(())
}

/// Start devnet if not already running
async fn start_devnet_if_needed() -> HylixResult<()> {
    // TODO: Check if devnet is already running
    // For now, we'll assume it needs to be started
    
    log_info("Starting local devnet...");
    
    // This would typically involve:
    // 1. Starting the local Hyli node
    // 2. Deploying the Oranj token contract
    // 3. Setting up the wallet app
    // 4. Starting the indexer
    // 5. Starting the explorer
    // 6. Creating pre-funded test accounts
    
    // Placeholder implementation
    std::thread::sleep(std::time::Duration::from_millis(1000));
    
    Ok(())
}

/// Build the project
async fn build_project() -> HylixResult<()> {
    // Build contracts
    let output = Command::new("cargo")
        .current_dir("contracts")
        .args(["build", "--release"])
        .output()
        .map_err(|e| HylixError::test(format!("Failed to build contracts: {}", e)))?;

    if !output.status.success() {
        let error_msg = String::from_utf8_lossy(&output.stderr);
        return Err(HylixError::test(format!(
            "Failed to build contracts: {}",
            error_msg
        )));
    }

    // Build server
    let output = Command::new("cargo")
        .current_dir("server")
        .args(["build", "--release"])
        .output()
        .map_err(|e| HylixError::test(format!("Failed to build server: {}", e)))?;

    if !output.status.success() {
        let error_msg = String::from_utf8_lossy(&output.stderr);
        return Err(HylixError::test(format!(
            "Failed to build server: {}",
            error_msg
        )));
    }

    Ok(())
}

/// Start the backend service
async fn start_backend() -> HylixResult<tokio::process::Child> {
    log_info("Starting backend service...");
    
    // Start the backend in the background
    let mut backend = tokio::process::Command::new("cargo")
        .current_dir("server")
        .args(["run", "--release"])
        .spawn()
        .map_err(|e| HylixError::test(format!("Failed to start backend: {}", e)))?;

    // Give the backend time to start up
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Check if the backend is still running
    match backend.try_wait() {
        Ok(Some(status)) => {
            return Err(HylixError::test(format!(
                "Backend exited unexpectedly with status: {}",
                status
            )));
        }
        Ok(None) => {
            log_info("Backend started successfully");
        }
        Err(e) => {
            return Err(HylixError::test(format!(
                "Failed to check backend status: {}",
                e
            )));
        }
    }

    Ok(backend)
}

/// Run the tests
async fn run_tests() -> HylixResult<()> {
    // Run unit tests in contracts
    let output = Command::new("cargo")
        .current_dir("contracts")
        .args(["test"])
        .output()
        .map_err(|e| HylixError::test(format!("Failed to run contract tests: {}", e)))?;

    if !output.status.success() {
        let error_msg = String::from_utf8_lossy(&output.stderr);
        return Err(HylixError::test(format!(
            "Contract tests failed: {}",
            error_msg
        )));
    }

    // Run unit tests in server
    let output = Command::new("cargo")
        .current_dir("server")
        .args(["test"])
        .output()
        .map_err(|e| HylixError::test(format!("Failed to run server tests: {}", e)))?;

    if !output.status.success() {
        let error_msg = String::from_utf8_lossy(&output.stderr);
        return Err(HylixError::test(format!(
            "Server tests failed: {}",
            error_msg
        )));
    }

    // Run E2E tests if they exist
    if std::path::Path::new("tests").exists() {
        let output = Command::new("cargo")
            .args(["test"])
            .output()
            .map_err(|e| HylixError::test(format!("Failed to run E2E tests: {}", e)))?;

        if !output.status.success() {
            let error_msg = String::from_utf8_lossy(&output.stderr);
            return Err(HylixError::test(format!(
                "E2E tests failed: {}",
                error_msg
            )));
        }
    }

    Ok(())
}

/// Cleanup resources
async fn cleanup(mut backend_handle: tokio::process::Child) -> HylixResult<()> {
    log_info("Stopping backend...");
    
    // Kill the backend process
    if let Err(e) = backend_handle.kill().await {
        log_error(&format!("Failed to kill backend process: {}", e));
    }

    // Wait for the process to exit
    if let Err(e) = backend_handle.wait().await {
        log_error(&format!("Error waiting for backend to exit: {}", e));
    }

    log_info("Stopping devnet...");
    
    // TODO: Stop the devnet
    // This would involve stopping all the services started in start_devnet_if_needed
    
    Ok(())
}

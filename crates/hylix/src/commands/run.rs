use crate::error::{HylixError, HylixResult};
use crate::logging::{log_success, log_info, log_error};
use std::process::Command;

/// Execute the `hy run` command
pub async fn execute(testnet: bool, watch: bool) -> HylixResult<()> {
    if testnet {
        log_info("Starting backend in testnet mode...");
    } else {
        log_info("Starting backend in local dev mode...");
    }

    if watch {
        log_info("Watch mode enabled (auto-rebuild on file changes)");
    }

    // Check if we're in a valid project directory
    validate_project_directory()?;

    // Build the project first
    log_info("Building project...");
    build_project().await?;

    // Start the backend
    if watch {
        run_with_watch(testnet).await?;
    } else {
        run_backend(testnet).await?;
    }

    Ok(())
}

/// Validate that we're in a valid project directory
fn validate_project_directory() -> HylixResult<()> {
    if !std::path::Path::new("server").exists() {
        return Err(HylixError::project(
            "No 'server' directory found. Are you in a Hylix project directory?"
        ));
    }

    Ok(())
}

/// Build the project
async fn build_project() -> HylixResult<()> {
    // Build contracts first
    if std::path::Path::new("contracts").exists() {
        let output = Command::new("cargo")
            .current_dir("contracts")
            .args(["build", "--release"])
            .output()
            .map_err(|e| HylixError::build(format!("Failed to build contracts: {}", e)))?;

        if !output.status.success() {
            let error_msg = String::from_utf8_lossy(&output.stderr);
            return Err(HylixError::build(format!(
                "Failed to build contracts: {}",
                error_msg
            )));
        }
    }

    // Build server
    let output = Command::new("cargo")
        .current_dir("server")
        .args(["build", "--release"])
        .output()
        .map_err(|e| HylixError::build(format!("Failed to build server: {}", e)))?;

    if !output.status.success() {
        let error_msg = String::from_utf8_lossy(&output.stderr);
        return Err(HylixError::build(format!(
            "Failed to build server: {}",
            error_msg
        )));
    }

    Ok(())
}

/// Run the backend service
async fn run_backend(testnet: bool) -> HylixResult<()> {
    log_info("Starting backend service...");
    
    let mut args = vec!["run", "--release"];
    if testnet {
        args.push("--");
        args.push("--testnet");
    }

    let mut backend = Command::new("cargo")
        .current_dir("server")
        .args(&args)
        .spawn()
        .map_err(|e| HylixError::backend(format!("Failed to start backend: {}", e)))?;

    log_success("Backend started successfully!");
    log_info("Backend is running. Press Ctrl+C to stop.");

    // Wait for the backend process
    match backend.wait() {
        Ok(status) => {
            if status.success() {
                log_info("Backend stopped gracefully");
            } else {
                log_error(&format!("Backend exited with error: {}", status));
            }
        }
        Err(e) => {
            return Err(HylixError::backend(format!(
                "Error waiting for backend: {}",
                e
            )));
        }
    }

    Ok(())
}

/// Run the backend with watch mode
async fn run_with_watch(testnet: bool) -> HylixResult<()> {
    log_info("Starting backend with watch mode...");
    
    // Check if cargo-watch is available
    if which::which("cargo-watch").is_err() {
        log_info("Installing cargo-watch for watch mode...");
        install_cargo_watch().await?;
    }

    let mut args = vec!["watch", "-x", "run --release"];
    if testnet {
        args.push("--");
        args.push("--testnet");
    }

    let mut backend = Command::new("cargo")
        .current_dir("server")
        .args(&args)
        .spawn()
        .map_err(|e| HylixError::backend(format!("Failed to start backend with watch: {}", e)))?;

    log_success("Backend started with watch mode!");
    log_info("Backend will automatically rebuild and restart on file changes.");
    log_info("Press Ctrl+C to stop.");

    // Wait for the backend process
    match backend.wait() {
        Ok(status) => {
            if status.success() {
                log_info("Backend stopped gracefully");
            } else {
                log_error(&format!("Backend exited with error: {}", status));
            }
        }
        Err(e) => {
            return Err(HylixError::backend(format!(
                "Error waiting for backend: {}",
                e
            )));
        }
    }

    Ok(())
}

/// Install cargo-watch for watch mode
async fn install_cargo_watch() -> HylixResult<()> {
    let output = Command::new("cargo")
        .args(["install", "cargo-watch"])
        .output()
        .map_err(|e| HylixError::backend(format!("Failed to install cargo-watch: {}", e)))?;

    if !output.status.success() {
        let error_msg = String::from_utf8_lossy(&output.stderr);
        return Err(HylixError::backend(format!(
            "Failed to install cargo-watch: {}",
            error_msg
        )));
    }

    Ok(())
}

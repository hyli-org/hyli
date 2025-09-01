use crate::error::{HylixError, HylixResult};
use crate::logging::{create_progress_bar, log_success, log_info};
use std::process::Command;

/// Execute the `hy build` command
pub async fn execute(clean: bool) -> HylixResult<()> {
    log_info("Building vApp project...");

    if clean {
        log_info("Cleaning build artifacts...");
        clean_build_artifacts().await?;
    }

    // Check if we're in a valid project directory
    validate_project_directory()?;

    // Build contracts
    let pb = create_progress_bar("Building contracts...");
    build_contracts().await?;
    pb.finish_with_message("Contracts built successfully");

    // Build server
    let pb = create_progress_bar("Building server...");
    build_server().await?;
    pb.finish_with_message("Server built successfully");

    // Build frontend (if exists)
    if std::path::Path::new("front").exists() {
        let pb = create_progress_bar("Building frontend...");
        build_frontend().await?;
        pb.finish_with_message("Frontend built successfully");
    }

    log_success("Build completed successfully!");
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

    Ok(())
}

/// Clean build artifacts
async fn clean_build_artifacts() -> HylixResult<()> {
    // Clean contracts
    if std::path::Path::new("contracts").exists() {
        let output = Command::new("cargo")
            .current_dir("contracts")
            .args(&["clean"])
            .output()
            .map_err(|e| HylixError::build(format!("Failed to clean contracts: {}", e)))?;

        if !output.status.success() {
            let error_msg = String::from_utf8_lossy(&output.stderr);
            return Err(HylixError::build(format!(
                "Failed to clean contracts: {}",
                error_msg
            )));
        }
    }

    // Clean server
    if std::path::Path::new("server").exists() {
        let output = Command::new("cargo")
            .current_dir("server")
            .args(&["clean"])
            .output()
            .map_err(|e| HylixError::build(format!("Failed to clean server: {}", e)))?;

        if !output.status.success() {
            let error_msg = String::from_utf8_lossy(&output.stderr);
            return Err(HylixError::build(format!(
                "Failed to clean server: {}",
                error_msg
            )));
        }
    }

    // Clean frontend
    if std::path::Path::new("front").exists() {
        let output = Command::new("bun")
            .current_dir("front")
            .args(&["run", "clean"])
            .output()
            .map_err(|e| HylixError::build(format!("Failed to clean frontend: {}", e)))?;

        if !output.status.success() {
            // Frontend clean might not be implemented, that's okay
            log_info("Frontend clean command not available, skipping...");
        }
    }

    Ok(())
}

/// Build contracts
async fn build_contracts() -> HylixResult<()> {
    let output = Command::new("cargo")
        .current_dir("contracts")
        .args(&["build", "--release"])
        .output()
        .map_err(|e| HylixError::build(format!("Failed to build contracts: {}", e)))?;

    if !output.status.success() {
        let error_msg = String::from_utf8_lossy(&output.stderr);
        return Err(HylixError::build(format!(
            "Failed to build contracts: {}",
            error_msg
        )));
    }

    Ok(())
}

/// Build server
async fn build_server() -> HylixResult<()> {
    let output = Command::new("cargo")
        .current_dir("server")
        .args(&["build", "--release"])
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

/// Build frontend
async fn build_frontend() -> HylixResult<()> {
    // Check if bun is available
    if which::which("bun").is_err() {
        log_info("Bun not found, skipping frontend build");
        return Ok(());
    }

    let output = Command::new("bun")
        .current_dir("front")
        .args(&["run", "build"])
        .output()
        .map_err(|e| HylixError::build(format!("Failed to build frontend: {}", e)))?;

    if !output.status.success() {
        let error_msg = String::from_utf8_lossy(&output.stderr);
        return Err(HylixError::build(format!(
            "Failed to build frontend: {}",
            error_msg
        )));
    }

    Ok(())
}

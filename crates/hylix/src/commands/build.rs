use crate::error::{HylixError, HylixResult};
use crate::logging::{create_progress_bar, execute_command_with_progress, log_info, log_success};
use std::process::Command;

/// Execute the `hy build` command
pub async fn execute(clean: bool, frontend: bool) -> HylixResult<()> {
    log_info("Building vApp project...");

    if clean {
        log_info("Cleaning build artifacts...");
        clean_build_artifacts().await?;
    }

    // Check if we're in a valid project directory
    validate_project_directory()?;

    // Build contracts
    let mpb = indicatif::MultiProgress::new();
    build_contracts(&mpb).await?;
    log_success("Contracts built successfully");

    // Build server
    build_server(&mpb).await?;
    log_success("Server built successfully");

    // Build frontend (if exists)
    if frontend && std::path::Path::new("front").exists() {
        build_frontend(&mpb).await?;
        log_success("Frontend built successfully");
    }

    log_success("Build completed successfully!");
    Ok(())
}

/// Validate that we're in a valid project directory
fn validate_project_directory() -> HylixResult<()> {
    if !std::path::Path::new("contracts").exists() {
        return Err(HylixError::project(
            "No 'contracts' directory found. Are you in a Hylix project directory?",
        ));
    }

    if !std::path::Path::new("server").exists() {
        return Err(HylixError::project(
            "No 'server' directory found. Are you in a Hylix project directory?",
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
            .args(["clean"])
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
            .args(["clean"])
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
            .args(["run", "clean"])
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
async fn build_contracts(mpb: &indicatif::MultiProgress) -> HylixResult<()> {
    let pb = mpb.add(create_progress_bar());
    pb.set_message("Building contracts...");

    let success = execute_command_with_progress(
        mpb,
        "cargo build -p contracts --features nonreproducible",
        "cargo",
        &[
            "build",
            "-p",
            "contracts",
            "--features",
            "nonreproducible",
        ],
        None,
    )
    .await?;

    if !success {
        return Err(HylixError::build("Failed to build contracts".to_string()));
    } else {
        pb.set_message("Contracts built successfully");
    }

    mpb.clear()
        .map_err(|e| HylixError::process(format!("Failed to clear progress bars: {}", e)))?;

    Ok(())
}

/// Build server
async fn build_server(mpb: &indicatif::MultiProgress) -> HylixResult<()> {
    let pb = mpb.add(create_progress_bar());
    pb.set_message("Building server...");

    let success = execute_command_with_progress(
        mpb,
        "cargo build -p server --features nonreproducible",
        "cargo",
        &["build", "-p", "server", "--features", "nonreproducible"],
        None,
    )
    .await?;

    if !success {
        return Err(HylixError::build("Failed to build server".to_string()));
    } else {
        pb.set_message("Server built successfully");
    }

    mpb.clear()
        .map_err(|e| HylixError::process(format!("Failed to clear progress bars: {}", e)))?;

    Ok(())
}

/// Build frontend
async fn build_frontend(mpb: &indicatif::MultiProgress) -> HylixResult<()> {
    // Check if bun is available
    if which::which("bun").is_err() {
        log_info("Bun not found, skipping frontend build");
        return Ok(());
    }

    let pb = mpb.add(create_progress_bar());
    pb.set_message("Building frontend...");

    // bun install
    let success =
        execute_command_with_progress(mpb, "bun install", "bun", &["install"], Some("front"))
            .await?;

    if !success {
        return Err(HylixError::build(
            "Failed to install frontend dependencies".to_string(),
        ));
    } else {
        pb.set_message("Frontend dependencies installed successfully");
    }

    // bun run build
    let success = execute_command_with_progress(
        mpb,
        "bun run build",
        "bun",
        &["run", "build"],
        Some("front"),
    )
    .await?;

    if !success {
        return Err(HylixError::build("Failed to build frontend".to_string()));
    } else {
        pb.set_message("Frontend built successfully");
    }

    mpb.clear()
        .map_err(|e| HylixError::process(format!("Failed to clear progress bars: {}", e)))?;

    Ok(())
}

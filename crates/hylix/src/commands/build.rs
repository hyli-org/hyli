use crate::error::{HylixError, HylixResult};
use crate::logging::{log_info, log_success, ProgressExecutor};
use crate::process::execute_and_check;
use crate::validation::{validate_project, ValidationLevel};

/// Execute the `hy build` command
pub async fn execute(clean: bool, frontend: bool) -> HylixResult<()> {
    log_info("Building vApp project...");

    if clean {
        log_info("Cleaning build artifacts...");
        clean_build_artifacts().await?;
    }

    // Check if we're in a valid project directory
    validate_project(ValidationLevel::Build)?;

    // Build contracts
    let executor = ProgressExecutor::new();
    build_contracts(&executor).await?;
    log_success("Contracts built successfully");

    // Build server
    build_server(&executor).await?;
    log_success("Server built successfully");

    // Build frontend (if exists)
    if frontend && std::path::Path::new("front").exists() {
        build_frontend(&executor).await?;
        log_success("Frontend built successfully");
    }

    executor.clear()?;

    log_success("Build completed successfully!");
    Ok(())
}

/// Clean build artifacts
async fn clean_build_artifacts() -> HylixResult<()> {
    // Clean contracts
    if std::path::Path::new("contracts").exists() {
        execute_and_check(
            "cargo",
            &["clean"],
            Some("contracts"),
            "Failed to clean contracts",
        )
        .await?;
    }

    // Clean server
    if std::path::Path::new("server").exists() {
        execute_and_check(
            "cargo",
            &["clean"],
            Some("server"),
            "Failed to clean server",
        )
        .await?;
    }

    // Clean frontend
    if std::path::Path::new("front").exists()
        && execute_and_check(
            "bun",
            &["run", "clean"],
            Some("front"),
            "Failed to clean frontend",
        )
        .await
        .is_err()
    {
        // Frontend clean might not be implemented, that's okay
        log_info("Frontend clean command not available, skipping...");
    }

    Ok(())
}

/// Build contracts
async fn build_contracts(executor: &ProgressExecutor) -> HylixResult<()> {
    let success = executor
        .execute_command(
            "Building contracts...",
            "cargo",
            &[
                "build",
                "-p",
                "contracts",
                "--features",
                crate::constants::features::NONREPRODUCIBLE,
            ],
            None,
        )
        .await?;

    if !success {
        return Err(HylixError::build("Failed to build contracts".to_string()));
    }

    executor.clear()?;

    Ok(())
}

/// Build server
async fn build_server(executor: &ProgressExecutor) -> HylixResult<()> {
    let success = executor
        .execute_command(
            "Building server...",
            "cargo",
            &[
                "build",
                "-p",
                "server",
                "--features",
                crate::constants::features::NONREPRODUCIBLE,
            ],
            None,
        )
        .await?;

    if !success {
        return Err(HylixError::build("Failed to build server".to_string()));
    }

    executor.clear()?;

    Ok(())
}

/// Build frontend
async fn build_frontend(executor: &ProgressExecutor) -> HylixResult<()> {
    // Check if bun is available
    if which::which("bun").is_err() {
        log_info("Bun not found, skipping frontend build");
        return Ok(());
    }

    // bun install
    let success = executor
        .execute_command(
            "Installing frontend dependencies",
            "bun",
            &["install"],
            Some("front"),
        )
        .await?;

    if !success {
        return Err(HylixError::build(
            "Failed to install frontend dependencies".to_string(),
        ));
    }

    // bun run build
    let success = executor
        .execute_command("Building frontend", "bun", &["run", "build"], Some("front"))
        .await?;

    if !success {
        return Err(HylixError::build("Failed to build frontend".to_string()));
    }

    executor.clear()?;

    Ok(())
}

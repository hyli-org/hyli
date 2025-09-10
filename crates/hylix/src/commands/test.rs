use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};

use crate::commands;
use crate::commands::devnet::DevnetAction;
use crate::error::{HylixError, HylixResult};
use crate::logging::{create_progress_bar_with_msg, log_error, log_info, log_success};

/// Execute the `hy test` command
pub async fn execute(keep_alive: bool, e2e: bool, unit: bool) -> HylixResult<()> {
    // Validate flags - can't run both e2e and unit only at the same time
    if e2e && unit {
        return Err(HylixError::test(
            "Cannot specify both --e2e and --unit flags. Use --e2e for e2e tests only, --unit for unit tests only, or neither for both.".to_string(),
        ));
    }

    // Check if we're in a valid project directory
    validate_project_directory()?;

    // Build the project
    build_project().await?;

    // Determine which tests to run
    let run_unit = !e2e; // Run unit tests unless --e2e is specified
    let run_e2e = !unit; // Run e2e tests unless --unit is specified

    // Run unit tests if needed
    if run_unit {
        run_unit_tests().await?;
    }

    // Run e2e tests if needed
    if run_e2e {
        // Start devnet if not already running
        start_devnet_if_needed().await?;

        // Start backend for e2e tests
        let mut backend_handle = start_backend().await?;

        // Run e2e tests
        let result = run_e2e_tests().await;
        if let Err(e) = result {
            log_error(&format!("Failed to run e2e tests: {}", e));
            if let Err(e) = save_backend_logs(&mut backend_handle).await {
                log_error(&format!("Failed to save backend logs: {}", e));
            }
        }

        // Cleanup if not keeping devnet and backend alive
        if !keep_alive {
            let pb = create_progress_bar_with_msg("Cleaning up...");
            cleanup(backend_handle).await?;
            pb.finish_with_message("Cleanup completed");
        } else {
            log_info("Keeping devnet and backend alive as requested");
        }
    }

    log_success("All tests completed successfully!");
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

    if !std::path::Path::new("tests").exists() {
        log_info("No 'tests' directory found, running unit tests only");
    }

    Ok(())
}

/// Start devnet if not already running
async fn start_devnet_if_needed() -> HylixResult<()> {
    commands::devnet::execute(DevnetAction::Up {
        reset: false,
        bake: true,
        profile: None,
    })
    .await
}

/// Build the project
async fn build_project() -> HylixResult<()> {
    commands::build::execute(false, false).await
}

/// Start the backend service
async fn start_backend() -> HylixResult<tokio::process::Child> {
    let mut backend =
        commands::run::run_backend(false, &crate::config::HylixConfig::load()?, true).await?;

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

async fn run_unit_tests() -> HylixResult<()> {
    // Run unit tests in contracts
    log_info(&format!(
        "{}",
        console::style("-------------------- HYLIX CONTRACT TESTS --------------------").green()
    ));
    log_info(&format!(
        "{}",
        console::style("$ cargo test -p contracts").green()
    ));

    let status = std::process::Command::new("cargo")
        .args(["test", "-p", "contracts"])
        .status()
        .map_err(|e| HylixError::process(format!("Failed to run contract tests: {}", e)))?;

    if !status.success() {
        return Err(HylixError::process(
            "Failed to run contract tests".to_string(),
        ));
    }

    // Run unit tests in server
    log_info(&format!(
        "{}",
        console::style("-------------------- HYLIX SERVER TESTS --------------------").green()
    ));
    log_info(&format!(
        "{}",
        console::style("$ cargo test -p server").green()
    ));

    let status = std::process::Command::new("cargo")
        .args(["test", "-p", "server"])
        .status()
        .map_err(|e| HylixError::process(format!("Failed to run server tests: {}", e)))?;

    if !status.success() {
        return Err(HylixError::process(
            "Failed to run server tests".to_string(),
        ));
    }

    Ok(())
}

/// Run the e2e tests
async fn run_e2e_tests() -> HylixResult<()> {
    // Run E2E tests if they exist
    if std::path::Path::new("tests").exists() && std::path::Path::new("tests/package.json").exists()
    {
        log_info(&format!(
            "{}",
            console::style("-------------------- HYLIX E2E TESTS --------------------").green()
        ));
        log_info(&format!("{}", console::style("$ bun test").green()));

        let status = std::process::Command::new("bun")
            .args(["test"])
            .status()
            .map_err(|e| HylixError::process(format!("Failed to run E2E tests: {}", e)))?;

        if !status.success() {
            return Err(HylixError::process("Failed to run E2E tests".to_string()));
        }
    }

    Ok(())
}

/// Write backend logs to a new temporary file with unique name in working directory
async fn save_backend_logs(backend_handle: &mut tokio::process::Child) -> HylixResult<()> {
    let mut logs = String::new();
    let tmpdir = std::env::current_dir()
        .map_err(|e| HylixError::process(format!("Failed to get current directory: {}", e)))?;
    let file = tmpdir.join(format!(
        "backend_logs_{}.txt",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    ));
    let file_display = file.display().to_string();
    let reader = BufReader::new(
        backend_handle
            .stdout
            .take()
            .ok_or(HylixError::process("Failed to get stdout"))?,
    );
    let mut writer = BufWriter::new(
        File::create(file)
            .await
            .map_err(|e| HylixError::process(format!("Failed to create file: {}", e)))?,
    );

    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        logs.push_str(&line);
        logs.push_str("\n");
    }

    writer
        .write_all(logs.as_bytes())
        .await
        .map_err(|e| HylixError::process(format!("Failed to write backend logs: {}", e)))?;

    log_success(&format!("Backend logs saved to: {}", file_display));

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

    Ok(())
}

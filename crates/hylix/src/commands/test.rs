use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};

use crate::commands;
use crate::commands::devnet::DevnetAction;
use crate::error::{HylixError, HylixResult};
use crate::logging::{
    create_progress_bar, create_progress_bar_with_msg, log_error, log_info, log_success,
};

/// Execute the `hy test` command
pub async fn execute(
    keep_alive: bool,
    e2e: bool,
    unit: bool,
    extra_args: Vec<String>,
) -> HylixResult<()> {
    // Validate flags - can't run both e2e and unit only at the same time
    if e2e && unit {
        return Err(HylixError::test(
            "Cannot specify both --e2e and --unit flags. Use --e2e for e2e tests only, --unit for unit tests only, or neither for both.".to_string(),
        ));
    }

    // Check if we're in a valid project directory
    validate_project_directory()?;

    let config = crate::config::HylixConfig::load()?;

    // Build the project
    build_project().await?;

    // Determine which tests to run
    let run_unit = !e2e; // Run unit tests unless --e2e is specified
    let run_e2e = !unit; // Run e2e tests unless --unit is specified

    // Run unit tests if needed
    if run_unit {
        run_unit_tests(&extra_args).await?;
    }

    // Run e2e tests if needed
    if run_e2e {
        // Start devnet if not already running
        start_devnet_if_needed().await?;

        // Start backend for e2e tests
        let mut backend_handle = start_backend(&config, &[]).await?;

        // Run e2e tests
        let result = run_e2e_tests(&config).await;
        if let Err(e) = result {
            log_error(&format!("Failed to run e2e tests: {e}"));
        }

        // Cleanup if not keeping devnet and backend alive
        if !keep_alive {
            cleanup(&mut backend_handle).await?;
            if !config.test.print_server_logs {
                save_backend_logs(&mut backend_handle).await?;
            } else {
                log_info("Backend logs not saved to file (config test.print_server_logs is true)");
            }
        } else {
            log_info("Keeping backend alive as requested");
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
        no_pull: false,
    })
    .await
}

/// Build the project
async fn build_project() -> HylixResult<()> {
    commands::build::execute(false, false).await
}

/// Start the backend service
async fn start_backend(
    config: &crate::config::HylixConfig,
    extra_args: &[String],
) -> HylixResult<tokio::process::Child> {
    let mut backend = commands::run::run_backend(false, config, true, extra_args).await?;

    // Check if the backend is running by loop-polling /_health
    let mut attempts = 0;
    const MAX_ATTEMPTS: u32 = 50;
    let mpb = indicatif::MultiProgress::new();
    let pb = mpb.add(create_progress_bar());
    let pb2 = mpb.add(create_progress_bar());
    while !is_backend_running(config, &pb2).await {
        attempts += 1;
        pb.set_message(format!(
            "Waiting for backend to start... (attempt {attempts}/{MAX_ATTEMPTS})"
        ));

        if attempts >= MAX_ATTEMPTS {
            backend.kill().await?;
            return Err(HylixError::backend(
                "Backend failed to start under 50 seconds".to_string(),
            ));
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // Check if the backend process is still running
        match backend.try_wait() {
            Ok(Some(status)) => {
                return Err(HylixError::backend(format!(
                    "Backend exited unexpectedly with status: {status}"
                )));
            }
            Ok(None) => {}
            Err(e) => {
                return Err(HylixError::backend(format!(
                    "Failed to check backend status: {e}"
                )));
            }
        }
    }
    mpb.clear()
        .map_err(|e| HylixError::process(format!("Failed to clear progress bars: {e}")))?;

    log_success("Backend is ready to accept requests");
    Ok(backend)
}

/// Check if the backend is running by polling /_health
async fn is_backend_running(
    config: &crate::config::HylixConfig,
    pb: &indicatif::ProgressBar,
) -> bool {
    let response = reqwest::get(format!(
        "http://localhost:{}/_health",
        config.run.server_port
    ))
    .await;

    match response {
        Ok(response) => {
            pb.set_message("Backend is running");
            response.status() == 200
        }
        Err(e) => {
            pb.set_message(format!("Backend not responding: {e}"));
            false
        }
    }
}

async fn run_unit_tests(extra_args: &[String]) -> HylixResult<()> {
    // Run unit tests in contracts
    log_info(&format!(
        "{}",
        console::style("-------------------- HYLIX UNIT TESTS --------------------").green()
    ));

    // Build the command display string
    let mut cmd_display = String::from("$ cargo test");
    for arg in extra_args {
        cmd_display.push(' ');
        cmd_display.push_str(arg);
    }
    log_info(&format!("{}", console::style(&cmd_display).green()));

    // Build the cargo test command with extra arguments
    let mut cmd = std::process::Command::new("cargo");
    cmd.arg("test");
    cmd.args(extra_args);

    let status = cmd
        .status()
        .map_err(|e| HylixError::process(format!("Failed to run unit tests: {e}")))?;

    if !status.success() {
        return Err(HylixError::process("Failed to run unit tests".to_string()));
    }

    Ok(())
}

/// Run the e2e tests
async fn run_e2e_tests(config: &crate::config::HylixConfig) -> HylixResult<()> {
    // Run E2E tests if they exist
    if std::path::Path::new("tests").exists() && std::path::Path::new("tests/package.json").exists()
    {
        log_info(&format!(
            "{}",
            console::style("-------------------- HYLIX E2E TESTS --------------------").green()
        ));
        log_info(&format!(
            "{}",
            console::style(&format!(
                "$ API_BASE_URL=http://localhost:{} bun test",
                config.run.server_port
            ))
            .green()
        ));

        let status = std::process::Command::new("bun")
            .args(["test"])
            .env(
                "API_BASE_URL",
                format!("http://localhost:{}", config.run.server_port),
            )
            .status()
            .map_err(|e| HylixError::process(format!("Failed to run E2E tests: {e}")))?;

        if !status.success() {
            return Err(HylixError::process("Failed to run E2E tests".to_string()));
        }
    }

    Ok(())
}

/// Write backend logs to a new temporary file with unique name in working directory
async fn save_backend_logs(backend_handle: &mut tokio::process::Child) -> HylixResult<()> {
    let pb = create_progress_bar_with_msg("Saving backend logs...");
    let mut logs = String::new();
    let tmpdir = std::env::current_dir()
        .map_err(|e| HylixError::process(format!("Failed to get current directory: {e}")))?;
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
    pb.set_message(format!("Saving backend logs to: {file_display}"));
    let mut writer = BufWriter::new(
        File::create(file)
            .await
            .map_err(|e| HylixError::process(format!("Failed to create file: {e}")))?,
    );

    let mut lines = reader.lines();
    let mut line_count = 0;
    while let Ok(Some(line)) = lines.next_line().await {
        pb.set_message(format!("Saving backend logs... ({line_count} lines)"));
        line_count += 1;
        logs.push_str(&line);
        logs.push('\n');
    }

    pb.set_message(format!(
        "Saved {line_count} lines to buffer. Writing to file..."
    ));
    writer
        .write_all(logs.as_bytes())
        .await
        .map_err(|e| HylixError::process(format!("Failed to write backend logs: {e}")))?;

    pb.finish_and_clear();

    log_success(&format!("Backend logs saved to: {file_display}"));

    Ok(())
}

/// Cleanup resources
async fn cleanup(backend_handle: &mut tokio::process::Child) -> HylixResult<()> {
    // Kill the backend process
    if let Err(e) = backend_handle.kill().await {
        log_error(&format!("Failed to kill backend process: {e}"));
    }

    // Wait for the process to exit
    if let Err(e) = backend_handle.wait().await {
        log_error(&format!("Error waiting for backend to exit: {e}"));
    }

    log_success("Backend stopped");

    Ok(())
}

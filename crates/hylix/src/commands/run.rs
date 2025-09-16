use crate::config::HylixConfig;
use crate::error::{HylixError, HylixResult};
use crate::logging::{log_error, log_info, log_success};
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

    let config = HylixConfig::load()?;

    // Check if we're in a valid project directory
    validate_project_directory()?;

    // Build the project first
    build_project(&config).await?;

    // Start the backend
    if watch {
        run_with_watch(testnet, &config).await?;
    } else {
        let backend = run_backend(testnet, &config, false).await?;
        wait_backend(backend).await?;
    }

    Ok(())
}

/// Validate that we're in a valid project directory
fn validate_project_directory() -> HylixResult<()> {
    if !std::path::Path::new("server").exists() {
        return Err(HylixError::project(
            "No 'server' directory found. Are you in a Hylix project directory?",
        ));
    }

    Ok(())
}

/// Build the project
async fn build_project(_config: &crate::config::HylixConfig) -> HylixResult<()> {
    crate::commands::build::execute(false, false).await
}

/// Run the backend service
pub async fn run_backend(
    testnet: bool,
    config: &crate::config::HylixConfig,
    for_testing: bool,
) -> HylixResult<tokio::process::Child> {
    let server_port = config.run.server_port.to_string();
    let mut args = vec![
        "run",
        "-p",
        "server",
        "-F",
        "nonreproducible",
        "--",
        "--server-port",
        &server_port,
    ];
    if testnet {
        args.push("--testnet");
    }
    if for_testing && config.test.clean_server_data {
        args.push("--clean-data-directory");
    }

    let print_logs = !for_testing || config.test.print_server_logs;

    log_info(&format!(
        "{}",
        console::style(&format!("$ cargo {}", args.join(" "))).green()
    ));

    let backend = tokio::process::Command::new("cargo")
        .env("RISC0_DEV_MODE", "1")
        .env(
            "HYLI_NODE_URL",
            format!("http://localhost:{}", config.devnet.node_port),
        )
        .env(
            "HYLI_INDEXER_URL",
            format!("http://localhost:{}", config.devnet.indexer_port),
        )
        .env(
            "HYLI_DA_READ_FROM",
            format!("localhost:{}", config.devnet.da_port),
        )
        .stdout(if print_logs {
            std::process::Stdio::inherit()
        } else {
            std::process::Stdio::piped()
        })
        .stderr(if print_logs {
            std::process::Stdio::inherit()
        } else {
            std::process::Stdio::piped()
        })
        .args(&args)
        .spawn()
        .map_err(|e| HylixError::backend(format!("Failed to start backend: {}", e)))?;

    log_success("Backend started successfully!");
    log_info("Backend is running. Press Ctrl+C to stop.");
    if !print_logs {
        log_info("Backend logs will not be printed to console. They will be saved to a file in the working directory.");
        log_info(&format!(
            "You can change this with `{}`.",
            console::style("hy config edit test.print_server_logs true").green()
        ));
    } else {
        log_info("Backend logs will be printed to console.");
        log_info(&format!(
            "You can change this with `{}`.",
            console::style("hy config edit test.print_server_logs false").green()
        ));
    }

    Ok(backend)
}

/// Wait for the backend process
async fn wait_backend(mut backend: tokio::process::Child) -> HylixResult<()> {
    match backend.wait().await {
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
async fn run_with_watch(testnet: bool, config: &crate::config::HylixConfig) -> HylixResult<()> {
    log_info("Starting backend with watch mode...");

    // Check if cargo-watch is available
    if which::which("cargo-watch").is_err() {
        log_info("Installing cargo-watch for watch mode...");
        install_cargo_watch().await?;
    }

    let mut args = vec!["watch", "-x", "run -p server"];
    if testnet {
        args.push("--");
        args.push("--testnet");
    }

    let mut backend = Command::new("cargo")
        .env("RISC0_DEV_MODE", "1")
        .env(
            "HYLI_NODE_URL",
            format!("http://localhost:{}", config.devnet.node_port),
        )
        .env(
            "HYLI_INDEXER_URL",
            format!("http://localhost:{}", config.devnet.indexer_port),
        )
        .env(
            "HYLI_DA_READ_FROM",
            format!("localhost:{}", config.devnet.da_port),
        )
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

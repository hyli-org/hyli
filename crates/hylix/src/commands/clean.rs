use crate::error::HylixResult;
use crate::logging::{log_info, log_success};
use crate::process::execute_and_check;
use crate::validation::{ValidationLevel, validate_project};
use std::path::Path;

/// Execute the `hy clean` command
pub async fn execute() -> HylixResult<()> {
    log_info("Cleaning build artifacts...");

    // Check if we're in a valid project directory
    validate_project(ValidationLevel::Clean)?;

    // Clean contracts
    if Path::new("contracts").exists() {
        log_info("Cleaning contracts...");
        clean_contracts().await?;
    }

    // Clean server
    if Path::new("server").exists() {
        log_info("Cleaning server...");
        clean_server().await?;
    }

    // Clean frontend
    if Path::new("front").exists() {
        log_info("Cleaning frontend...");
        clean_frontend().await?;
    }

    // Clean test artifacts
    if Path::new("tests").exists() {
        log_info("Cleaning test artifacts...");
        clean_test_artifacts().await?;
    }

    log_success("Clean completed successfully!");
    Ok(())
}

/// Clean contracts build artifacts
async fn clean_contracts() -> HylixResult<()> {
    execute_and_check(
        "cargo",
        &["clean"],
        Some("contracts"),
        "Failed to clean contracts",
    )
    .await?;

    // Also clean any additional build artifacts specific to contracts
    clean_contract_specific_artifacts().await?;

    Ok(())
}

/// Clean server build artifacts
async fn clean_server() -> HylixResult<()> {
    execute_and_check(
        "cargo",
        &["clean"],
        Some("server"),
        "Failed to clean server",
    )
    .await?;

    // Also clean any additional build artifacts specific to server
    clean_server_specific_artifacts().await?;

    Ok(())
}

/// Clean frontend build artifacts
async fn clean_frontend() -> HylixResult<()> {
    // Try to clean with bun if available
    if which::which("bun").is_ok()
        && execute_and_check(
            "bun",
            &["run", "clean"],
            Some("front"),
            "Failed to clean frontend",
        )
        .await
        .is_ok()
    {
        return Ok(());
    }

    // Fallback: manually clean common frontend build directories
    let build_dirs = ["dist", "build", "out", ".next", "node_modules/.cache"];

    for dir in &build_dirs {
        let path = Path::new("front").join(dir);
        if path.exists() {
            std::fs::remove_dir_all(&path)?;
            log_info(&format!("Removed {}", path.display()));
        }
    }

    Ok(())
}

/// Clean test artifacts
async fn clean_test_artifacts() -> HylixResult<()> {
    // Clean any test-specific directories
    let test_dirs = ["test-results", "coverage", ".nyc_output"];

    for dir in &test_dirs {
        let path = Path::new(dir);
        if path.exists() {
            std::fs::remove_dir_all(path)?;
            log_info(&format!("Removed {}", path.display()));
        }
    }

    Ok(())
}

/// Clean contract-specific artifacts
async fn clean_contract_specific_artifacts() -> HylixResult<()> {
    // Clean SP1 artifacts
    let sp1_dirs = ["target/sp1", "sp1-artifacts"];
    for dir in &sp1_dirs {
        let path = Path::new("contracts").join(dir);
        if path.exists() {
            std::fs::remove_dir_all(&path)?;
            log_info(&format!("Removed SP1 artifacts: {}", path.display()));
        }
    }

    // Clean Risc0 artifacts
    let risc0_dirs = ["target/risc0", "risc0-artifacts"];
    for dir in &risc0_dirs {
        let path = Path::new("contracts").join(dir);
        if path.exists() {
            std::fs::remove_dir_all(&path)?;
            log_info(&format!("Removed Risc0 artifacts: {}", path.display()));
        }
    }

    // Clean proof artifacts
    let proof_dirs = ["proofs", "artifacts"];
    for dir in &proof_dirs {
        let path = Path::new("contracts").join(dir);
        if path.exists() {
            std::fs::remove_dir_all(&path)?;
            log_info(&format!("Removed proof artifacts: {}", path.display()));
        }
    }

    Ok(())
}

/// Clean server-specific artifacts
async fn clean_server_specific_artifacts() -> HylixResult<()> {
    // Clean server logs
    let log_dirs = ["logs", "log"];
    for dir in &log_dirs {
        let path = Path::new("server").join(dir);
        if path.exists() {
            std::fs::remove_dir_all(&path)?;
            log_info(&format!("Removed server logs: {}", path.display()));
        }
    }

    // Clean server data
    let data_dirs = ["data", "db", "cache"];
    for dir in &data_dirs {
        let path = Path::new("server").join(dir);
        if path.exists() {
            std::fs::remove_dir_all(&path)?;
            log_info(&format!("Removed server data: {}", path.display()));
        }
    }

    Ok(())
}

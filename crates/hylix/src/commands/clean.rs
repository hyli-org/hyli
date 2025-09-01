use crate::error::{HylixError, HylixResult};
use crate::logging::{create_progress_bar, log_success, log_info};
use std::path::Path;
use std::process::Command;

/// Execute the `hy clean` command
pub async fn execute() -> HylixResult<()> {
    log_info("Cleaning build artifacts...");

    // Check if we're in a valid project directory
    validate_project_directory()?;

    // Clean contracts
    if Path::new("contracts").exists() {
        let pb = create_progress_bar("Cleaning contracts...");
        clean_contracts().await?;
        pb.finish_with_message("Contracts cleaned");
    }

    // Clean server
    if Path::new("server").exists() {
        let pb = create_progress_bar("Cleaning server...");
        clean_server().await?;
        pb.finish_with_message("Server cleaned");
    }

    // Clean frontend
    if Path::new("front").exists() {
        let pb = create_progress_bar("Cleaning frontend...");
        clean_frontend().await?;
        pb.finish_with_message("Frontend cleaned");
    }

    // Clean test artifacts
    if Path::new("tests").exists() {
        let pb = create_progress_bar("Cleaning test artifacts...");
        clean_test_artifacts().await?;
        pb.finish_with_message("Test artifacts cleaned");
    }

    // Clean temporary files
    let pb = create_progress_bar("Cleaning temporary files...");
    clean_temp_files().await?;
    pb.finish_with_message("Temporary files cleaned");

    log_success("Clean completed successfully!");
    Ok(())
}

/// Validate that we're in a valid project directory
fn validate_project_directory() -> HylixResult<()> {
    // Check for at least one of the expected directories
    if !Path::new("contracts").exists() 
        && !Path::new("server").exists() 
        && !Path::new("front").exists() {
        return Err(HylixError::project(
            "No project directories found. Are you in a Hylix project directory?"
        ));
    }

    Ok(())
}

/// Clean contracts build artifacts
async fn clean_contracts() -> HylixResult<()> {
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

    // Also clean any additional build artifacts specific to contracts
    clean_contract_specific_artifacts().await?;

    Ok(())
}

/// Clean server build artifacts
async fn clean_server() -> HylixResult<()> {
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

    // Also clean any additional build artifacts specific to server
    clean_server_specific_artifacts().await?;

    Ok(())
}

/// Clean frontend build artifacts
async fn clean_frontend() -> HylixResult<()> {
    // Try to clean with bun if available
    if which::which("bun").is_ok() {
        let output = Command::new("bun")
            .current_dir("front")
            .args(&["run", "clean"])
            .output()
            .map_err(|e| HylixError::build(format!("Failed to clean frontend: {}", e)))?;

        if output.status.success() {
            return Ok(());
        }
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
    // Clean cargo test artifacts
    let _output = Command::new("cargo")
        .args(&["test", "--", "--nocapture"])
        .output()
        .map_err(|e| HylixError::build(format!("Failed to clean test artifacts: {}", e)))?;

    // Clean any test-specific directories
    let test_dirs = ["test-results", "coverage", ".nyc_output"];
    
    for dir in &test_dirs {
        let path = Path::new(dir);
        if path.exists() {
            std::fs::remove_dir_all(&path)?;
            log_info(&format!("Removed {}", path.display()));
        }
    }

    Ok(())
}

/// Clean temporary files
async fn clean_temp_files() -> HylixResult<()> {
    // Clean common temporary files
    let temp_files = [
        ".DS_Store",
        "Thumbs.db",
        "*.tmp",
        "*.temp",
        ".vscode/settings.json",
        ".idea/",
    ];

    for pattern in &temp_files {
        if pattern.ends_with('/') {
            // Directory pattern
            let dir_name = &pattern[..pattern.len() - 1];
            let path = Path::new(dir_name);
            if path.exists() {
                std::fs::remove_dir_all(&path)?;
                log_info(&format!("Removed {}", path.display()));
            }
        } else if pattern.contains('*') {
            // Glob pattern - would need glob crate for full implementation
            // For now, just handle common cases
            if *pattern == "*.tmp" || *pattern == "*.temp" {
                // This would require glob matching in a real implementation
                log_info("Skipping glob pattern matching (not implemented)");
            }
        } else {
            // Regular file
            let path = Path::new(pattern);
            if path.exists() {
                std::fs::remove_file(&path)?;
                log_info(&format!("Removed {}", path.display()));
            }
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

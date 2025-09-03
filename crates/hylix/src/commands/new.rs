use crate::config::BackendType;
use crate::error::{HylixError, HylixResult};
use crate::logging::{create_progress_bar_with_msg, log_success, log_info};
use std::path::Path;
use std::process::Command;

/// Execute the `hy new` command
pub async fn execute(project_name: String, backend: Option<BackendType>) -> HylixResult<()> {
    log_info(&format!("Creating new vApp project: {}", project_name));

    // Validate project name
    validate_project_name(&project_name)?;

    // Check if directory already exists
    if Path::new(&project_name).exists() {
        return Err(HylixError::project(format!(
            "Directory '{}' already exists",
            project_name
        )));
    }

    // Determine backend type
    let backend_type = backend.unwrap_or_else(|| {
        log_info("No backend specified, defaulting to SP1");
        BackendType::Sp1
    });

    log_info(&format!("Using backend: {:?}", backend_type));

    // Clone the scaffold repository
    let pb = create_progress_bar_with_msg("Cloning scaffold repository...");
    clone_scaffold(&project_name).await?;
    pb.finish_with_message("Scaffold cloned successfully");

    // Setup project for the chosen backend
    let pb = create_progress_bar_with_msg("Setting up project structure...");
    setup_backend(&project_name, &backend_type).await?;
    pb.finish_with_message("Project structure configured");

    // Initialize git repository
    let pb = create_progress_bar_with_msg("Initializing git repository...");
    init_git_repo(&project_name)?;
    pb.finish_with_message("Git repository initialized");

    log_success(&format!("Project '{}' created successfully!", project_name));
    log_info("Next steps:");
    log_info(&format!("  cd {}", project_name));
    log_info("  hy build");
    log_info("  hy devnet");
    log_info("  hy test");

    Ok(())
}

/// Validate the project name
fn validate_project_name(name: &str) -> HylixResult<()> {
    if name.is_empty() {
        return Err(HylixError::validation("Project name cannot be empty"));
    }

    if name.contains('/') || name.contains('\\') {
        return Err(HylixError::validation("Project name cannot contain path separators"));
    }

    if name.starts_with('.') {
        return Err(HylixError::validation("Project name cannot start with a dot"));
    }

    // Check if name is a valid Rust identifier
    if !name.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_') {
        return Err(HylixError::validation(
            "Project name can only contain alphanumeric characters, hyphens, and underscores"
        ));
    }

    Ok(())
}

/// Clone the scaffold repository
async fn clone_scaffold(project_name: &str) -> HylixResult<()> {
    let scaffold_url = "https://github.com/hyli-org/app-scaffold.git";
    
    let output = Command::new("git")
        .args(["clone", scaffold_url, project_name])
        .output()
        .map_err(|e| HylixError::process(format!("Failed to clone scaffold: {}", e)))?;

    if !output.status.success() {
        let error_msg = String::from_utf8_lossy(&output.stderr);
        return Err(HylixError::process(format!(
            "Failed to clone scaffold repository: {}",
            error_msg
        )));
    }

    Ok(())
}

/// Setup the project for the chosen backend
async fn setup_backend(project_name: &str, backend: &BackendType) -> HylixResult<()> {
    match backend {
        BackendType::Sp1 => {
            setup_sp1_backend(project_name).await?;
        }
        BackendType::Risc0 => {
            setup_risc0_backend(project_name).await?;
        }
    }

    Ok(())
}

/// Setup SP1 backend configuration
async fn setup_sp1_backend(_project_name: &str) -> HylixResult<()> {
    // TODO: Implement SP1-specific setup
    // This would include:
    // - Updating Cargo.toml with SP1 dependencies
    // - Setting up SP1-specific build configuration
    // - Creating SP1-specific contract templates
    log_info("Setting up SP1 backend configuration...");
    
    // Placeholder implementation
    std::thread::sleep(std::time::Duration::from_millis(500));
    
    Ok(())
}

/// Setup Risc0 backend configuration
async fn setup_risc0_backend(_project_name: &str) -> HylixResult<()> {
    // TODO: Implement Risc0-specific setup
    // This would include:
    // - Updating Cargo.toml with Risc0 dependencies
    // - Setting up Risc0-specific build configuration
    // - Creating Risc0-specific contract templates
    log_info("Setting up Risc0 backend configuration...");
    
    // Placeholder implementation
    std::thread::sleep(std::time::Duration::from_millis(500));
    
    Ok(())
}

/// Initialize git repository
fn init_git_repo(project_name: &str) -> HylixResult<()> {
    // Remove the existing .git directory from the scaffold
    let git_dir = Path::new(project_name).join(".git");
    if git_dir.exists() {
        std::fs::remove_dir_all(&git_dir)?;
    }

    // Initialize a new git repository
    let output = Command::new("git")
        .current_dir(project_name)
        .args(["init"])
        .output()
        .map_err(|e| HylixError::process(format!("Failed to initialize git: {}", e)))?;

    if !output.status.success() {
        let error_msg = String::from_utf8_lossy(&output.stderr);
        return Err(HylixError::process(format!(
            "Failed to initialize git repository: {}",
            error_msg
        )));
    }

    // Add initial commit
    let output = Command::new("git")
        .current_dir(project_name)
        .args(["add", "."])
        .output()
        .map_err(|e| HylixError::process(format!("Failed to add files to git: {}", e)))?;

    if !output.status.success() {
        let error_msg = String::from_utf8_lossy(&output.stderr);
        return Err(HylixError::process(format!(
            "Failed to add files to git: {}",
            error_msg
        )));
    }

    let output = Command::new("git")
        .current_dir(project_name)
        .args(["commit", "-m", "Initial commit from Hylix scaffold"])
        .output()
        .map_err(|e| HylixError::process(format!("Failed to create initial commit: {}", e)))?;

    if !output.status.success() {
        let error_msg = String::from_utf8_lossy(&output.stderr);
        return Err(HylixError::process(format!(
            "Failed to create initial commit: {}",
            error_msg
        )));
    }

    Ok(())
}

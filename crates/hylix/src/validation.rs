use crate::error::{HylixError, HylixResult};
use std::path::Path;

#[derive(Debug, Clone, Copy)]
pub enum ValidationLevel {
    Build, // Requires contracts/ and server/
    Run,   // Requires server/ only
    Test,  // Requires contracts/, server/, optional tests/
    Clean, // Requires at least one module
}

pub fn validate_project(level: ValidationLevel) -> HylixResult<()> {
    match level {
        ValidationLevel::Build => {
            require_dir("contracts")?;
            require_dir("server")?;
        }
        ValidationLevel::Run => {
            require_dir("server")?;
        }
        ValidationLevel::Test => {
            require_dir("contracts")?;
            require_dir("server")?;
            if !Path::new("tests").exists() {
                crate::logging::log_info("No 'tests' directory found, running unit tests only");
            }
        }
        ValidationLevel::Clean => {
            if !Path::new("contracts").exists()
                && !Path::new("server").exists()
                && !Path::new("front").exists()
            {
                return Err(HylixError::project(
                    "No project directories found. Are you in a Hylix project directory?",
                ));
            }
        }
    }
    Ok(())
}

fn require_dir(name: &str) -> HylixResult<()> {
    if !Path::new(name).exists() {
        return Err(HylixError::project(format!(
            "No '{name}' directory found. Are you in a Hylix project directory?"
        )));
    }
    Ok(())
}

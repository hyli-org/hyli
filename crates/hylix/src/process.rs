use crate::error::{HylixError, HylixResult};
use std::process::Output;

pub async fn execute_sync(
    program: &str,
    args: &[&str],
    current_dir: Option<&str>,
) -> HylixResult<Output> {
    let mut cmd = tokio::process::Command::new(program);

    if let Some(dir) = current_dir {
        cmd.current_dir(dir);
    }

    cmd.args(args)
        .output()
        .await
        .map_err(|e| HylixError::process(format!("Failed to execute {}: {}", program, e)))
}

pub async fn execute_and_check(
    program: &str,
    args: &[&str],
    current_dir: Option<&str>,
    error_msg: &str,
) -> HylixResult<()> {
    let output = execute_sync(program, args, current_dir).await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(HylixError::process(format!("{}: {}", error_msg, stderr)));
    }

    Ok(())
}

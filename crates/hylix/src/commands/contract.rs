use crate::commands::devnet::DevnetContext;
use crate::error::{HylixError, HylixResult};
use crate::logging::{
    create_progress_bar, execute_command_with_progress, log_info, log_success, log_warning,
};
use client_sdk::rest_client::NodeApiClient;
use std::time::Duration;

/// Contract action enumeration
#[derive(Debug, Clone)]
pub enum ContractAction {
    /// Delete a contract
    Delete { name: String },
}

/// Execute the `hy contract` command
pub async fn execute(action: ContractAction) -> HylixResult<()> {
    match action {
        ContractAction::Delete { name } => {
            delete_contract(&name).await?;
        }
    }

    Ok(())
}

/// Delete a contract from the network
async fn delete_contract(contract_name: &str) -> HylixResult<()> {
    // Create DevnetContext to check if devnet is running
    let config = crate::config::HylixConfig::load()?;
    let context = DevnetContext::new(config)?;

    // Check if devnet is running
    if !is_devnet_running(&context).await? {
        return Err(HylixError::devnet(
            "Devnet is not running. Please start the devnet first with 'hy devnet start'."
                .to_string(),
        ));
    }

    log_info(&format!("Deleting contract '{}'...", contract_name));

    // Create a multi-progress bar for the command execution
    let mpb = indicatif::MultiProgress::new();
    let pb = mpb.add(create_progress_bar());
    pb.set_message(format!("Deleting contract {}...", contract_name));

    // Execute the delete_contract command
    let task_name = format!("Delete contract {}", contract_name);
    let success = execute_command_with_progress(
        &mpb,
        &task_name,
        "npx",
        &[
            "--yes",
            "hyli-wallet-cli",
            "delete_contract",
            contract_name,
            "hylisecure",
        ],
        None,
    )
    .await?;

    // Clear progress bars
    mpb.clear()
        .map_err(|e| HylixError::process(format!("Failed to clear progress bars: {}", e)))?;

    if !success {
        log_warning(&format!(
            "Contract '{}' deletion completed with warnings",
            contract_name
        ));
        log_warning(&format!(
            "Command was: {}",
            console::style(format!(
                "npx hyli-wallet-cli delete_contract {} hylisecure",
                contract_name
            ))
            .green()
        ));
    } else {
        log_success(&format!(
            "Contract '{}' deleted successfully!",
            contract_name
        ));
    }

    Ok(())
}

/// Check if devnet is running
async fn is_devnet_running(context: &DevnetContext) -> HylixResult<bool> {
    // Use tokio::time::timeout to add a timeout to the async call
    match tokio::time::timeout(Duration::from_secs(5), context.client.get_block_height()).await {
        Ok(result) => match result {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        },
        Err(_) => Ok(false),
    }
}

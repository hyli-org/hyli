use crate::commands::devnet::DevnetContext;
use crate::error::{HylixError, HylixResult};
use crate::logging::{ProgressExecutor, log_info, log_success, log_warning};
use client_sdk::rest_client::NodeApiClient;
use std::time::Duration;

/// Contract action enumeration
#[derive(Debug, Clone, clap::Subcommand)]
pub enum ContractAction {
    /// Delete a contract
    #[command(alias = "d")]
    Delete {
        /// Contract name
        name: String,
    },
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

    log_info(&format!("Deleting contract '{contract_name}'..."));

    // Create a progress executor for the command execution
    let executor = ProgressExecutor::new();
    let success = executor
        .execute_command(
            format!("Delete contract {contract_name}"),
            "npx",
            &[
                "--yes",
                "hyli-wallet-cli",
                "delete_contract",
                contract_name,
                crate::constants::passwords::DEFAULT,
            ],
            None,
        )
        .await?;

    executor.clear()?;

    if !success {
        log_warning(&format!(
            "Contract '{contract_name}' deletion completed with warnings"
        ));
        log_warning(&format!(
            "Command was: {}",
            console::style(format!(
                "npx hyli-wallet-cli delete_contract {contract_name} {}",
                crate::constants::passwords::DEFAULT
            ))
            .green()
        ));
    } else {
        log_success(&format!("Contract '{contract_name}' deleted successfully!"));
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

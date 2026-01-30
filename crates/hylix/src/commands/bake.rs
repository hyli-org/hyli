use crate::commands::devnet::DevnetContext;
use crate::config::{AccountConfig, FundConfig};
use crate::error::{HylixError, HylixResult};
use crate::logging::{log_info, log_success, log_warning, ProgressExecutor};
use client_sdk::rest_client::NodeApiClient;
use std::time::Duration;

/// Execute the `hy bake` command
pub async fn bake_devnet(executor: &ProgressExecutor, context: &DevnetContext) -> HylixResult<()> {
    // Create default profile if it doesn't exist
    context.config.create_default_profile()?;

    // Determine which profile to use: CLI argument, context profile, or default
    let profile_name = context
        .profile
        .as_ref()
        .unwrap_or(&context.config.bake_profile);

    // Load the profile
    let profile = context.config.load_bake_profile(profile_name)?;

    // Check if devnet is running
    if !is_devnet_running(context).await? {
        return Err(HylixError::devnet(
            "Devnet is not running. Please start the devnet first with 'hy devnet start'."
                .to_string(),
        ));
    }

    // Create pre-funded test accounts
    create_test_accounts(executor, context, &profile.accounts).await?;
    log_success("[1/2] Test accounts created");

    // Send funds to test accounts
    send_funds_to_test_accounts(executor, context, &profile.funds).await?;
    log_success("[2/2] Funds sent to test accounts");

    log_success("Bake process completed successfully!");
    log_info(&format!("Using profile: {}", profile.name));
    log_info("Test accounts created:");
    for account in &profile.accounts {
        log_info(&format!(
            "  - {} (password: {})",
            account.name, account.password
        ));
    }
    log_info("Test accounts funded:");
    for fund in &profile.funds {
        log_info(&format!(
            "  - {} -> {} ({} {})",
            fund.from, fund.to, fund.amount, fund.token
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

/// Create pre-funded test accounts
pub async fn create_test_accounts(
    executor: &ProgressExecutor,
    _context: &DevnetContext,
    accounts: &[AccountConfig],
) -> HylixResult<()> {
    let main_pb = executor.add_task("Creating test accounts...");

    for account in accounts {
        main_pb.set_message(format!("Creating test account: {}...", account.name));
        let task_name = format!("Creating account {}", account.name);
        let success = executor
            .execute_command(
                task_name,
                "npx",
                &[
                    "--yes",
                    "hyli-wallet-cli",
                    "register",
                    &account.name,
                    &account.password,
                    &account.invite_code,
                ],
                None,
            )
            .await?;

        if !success {
            log_warning(&format!(
                "{} account creation completed with warnings",
                account.name
            ));
        } else {
            main_pb.set_message(format!("{} account created successfully", account.name));
        }
    }

    main_pb.finish_and_clear();

    Ok(())
}

/// Send funds to test accounts
pub async fn send_funds_to_test_accounts(
    executor: &ProgressExecutor,
    _context: &DevnetContext,
    funds: &[FundConfig],
) -> HylixResult<()> {
    let pb = executor.add_task("Sending funds to test accounts...");

    for fund in funds {
        pb.set_message(format!(
            "{} sending {} {} to {}...",
            fund.from, fund.amount, fund.token, fund.to
        ));

        let destination = format!("{}@wallet", fund.to);
        let amount_str = fund.amount.to_string();

        let task_name = format!("Funding account {}", fund.to);
        let success = executor
            .execute_command(
                task_name,
                "npx",
                &[
                    "hyli-wallet-cli",
                    "transfer",
                    &fund.from,
                    &fund.from_password,
                    &amount_str,
                    &fund.token,
                    &destination,
                ],
                None,
            )
            .await?;

        if !success {
            log_warning(&format!(
                "{} account funding completed with warnings",
                fund.to
            ));
            log_warning(&format!(
                "Command was: {}",
                console::style(format!(
                    "npx hyli-wallet-cli transfer {} {} {} {} {}",
                    fund.from, fund.from_password, amount_str, fund.token, destination
                ))
                .green()
            ));
        } else {
            pb.set_message(format!("{} account funded successfully", fund.to));
        }
    }

    pb.finish_and_clear();

    Ok(())
}

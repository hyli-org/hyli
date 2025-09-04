use crate::commands::devnet::DevnetContext;
use crate::config::{AccountConfig, FundConfig};
use crate::error::{HylixError, HylixResult};
use crate::logging::{create_progress_bar, execute_command_with_progress, log_info, log_success, log_warning};
use client_sdk::rest_client::{NodeApiClient};
use std::time::Duration;


/// Execute the `hy bake` command
pub async fn bake_devnet(mpb: &indicatif::MultiProgress, context: &DevnetContext) -> HylixResult<()> {
    // Create default profile if it doesn't exist
    context.config.create_default_profile()?;
    
    // Determine which profile to use: CLI argument, context profile, or default
    let profile_name = context.profile
        .as_ref()
        .unwrap_or(&context.config.bake_profile);
            
    // Load the profile
    let profile = context.config.load_bake_profile(profile_name)?;

    // Check if devnet is running
    if !is_devnet_running(&context).await? {
        return Err(HylixError::devnet(
            "Devnet is not running. Please start the devnet first with 'hy devnet start'.".to_string(),
        ));
    }

    // Create pre-funded test accounts
    create_test_accounts(&mpb, &context, &profile.accounts).await?;
    log_success("[1/2] Test accounts created");

    // Send funds to test accounts
    send_funds_to_test_accounts(&mpb, &context, &profile.funds).await?;
    log_success("[2/2] Funds sent to test accounts");

    log_success("Bake process completed successfully!");
    log_info(&format!("Using profile: {}", profile.name));
    log_info("Test accounts created:");
    for account in &profile.accounts {
        log_info(&format!("  - {} (password: {})", account.name, account.password));
    }
    log_info("Test accounts funded:");
    for fund in &profile.funds {
        log_info(&format!("  - {} -> {} ({} {})", fund.from, fund.to, fund.amount, fund.token));
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
    mpb: &indicatif::MultiProgress,
    _context: &DevnetContext,
    accounts: &[AccountConfig],
) -> HylixResult<()> {
    // Add the main progress bar to the multi-progress
    let main_pb = mpb.add(create_progress_bar());

    for account in accounts {
        let message = format!("Creating test account: {}...", account.name);
        main_pb.set_message(message);
        let task_name = format!("{} account creation", account.name);
        let success = execute_command_with_progress(
            &mpb,
            &task_name,
            "npx",
            &["--yes", "hyli-wallet-cli", "register", &account.name, &account.password, &account.invite_code],
            None
        ).await?;

        if !success {
            let warning_msg = format!("{} account creation completed with warnings", account.name);
            log_warning(&warning_msg);
        } else {
            let success_msg = format!("{} account created successfully", account.name);
            main_pb.set_message(success_msg);
        }
    }

    // Clear the main progress bar
    main_pb.finish_and_clear();

    // Clear all progress bars from the multi-progress
    mpb.clear().map_err(|e| HylixError::process(format!("Failed to clear progress bars: {}", e)))?;

    Ok(())
}

/// Send funds to test accounts
pub async fn send_funds_to_test_accounts(mpb: &indicatif::MultiProgress, _context: &DevnetContext, funds: &[FundConfig]) -> HylixResult<()> {
    let pb = mpb.add(create_progress_bar());
    pb.set_message("Sending funds to test accounts...");

    for fund in funds {
        let message = format!("{} sending {} {} to {}...", fund.from, fund.amount, fund.token, fund.to);
        pb.set_message(message);
        
        let task_name = format!("{} account funding", fund.to);
        let destination = format!("{}@wallet", fund.to);
        let amount_str = fund.amount.to_string();
        
        let success = execute_command_with_progress(
            &mpb,
            &task_name,
            "npx",
            &[
                "hyli-wallet-cli", 
                "transfer", 
                &fund.from, 
                &fund.from_password, 
                &amount_str, 
                &fund.token, 
                &destination
            ],
            None
        ).await?;

        if !success {
            let warning_msg = format!("{} account funding completed with warnings", fund.to);
            log_warning(&warning_msg);
        } else {
            let success_msg = format!("{} account funded successfully", fund.to);
            pb.set_message(success_msg);
        }
    }

    mpb.clear().map_err(|e| HylixError::process(format!("Failed to clear progress bars: {}", e)))?;

    Ok(())
}

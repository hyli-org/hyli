use crate::commands::devnet::DevnetContext;
use crate::error::{HylixError, HylixResult};
use crate::logging::{create_progress_bar, execute_command_with_progress, log_info, log_success, log_warning};
use client_sdk::rest_client::{NodeApiClient};
use std::time::Duration;


/// Execute the `hy bake` command
pub async fn bake_devnet(mpb: &indicatif::MultiProgress, context: &DevnetContext) -> HylixResult<()> {
    // Check if devnet is running
    if !is_devnet_running(&context).await? {
        return Err(HylixError::devnet(
            "Devnet is not running. Please start the devnet first with 'hy devnet start'.".to_string(),
        ));
    }

    // Create pre-funded test accounts
    create_test_accounts(&mpb, &context).await?;
    log_success("[1/2] Test accounts created");

    // Send funds to test accounts
    send_funds_to_test_accounts(&mpb, &context).await?;
    log_success("[2/2] Funds sent to test accounts");

    log_success("Bake process completed successfully!");
    log_info("Test accounts created:");
    log_info("  - Bob (password: hylisecure)");
    log_info("  - Alice (password: hylisecure)");
    log_info("Test accounts funded:");
    log_info("  - Bob (1000 oranj)");
    log_info("  - Alice (1000 oranj)");

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
) -> HylixResult<()> {
    // Add the main progress bar to the multi-progress
    let main_pb = mpb.add(create_progress_bar());

    // Create Bob account
    main_pb.set_message("Creating test account: Bob...");
    let bob_success = execute_command_with_progress(
        &mpb,
        "Bob account creation",
        "npx",
        &["--yes", "hyli-wallet-cli", "register", "bob", "hylisecure", "vip"]
    ).await?;

    if !bob_success {
        log_warning("Bob account creation completed with warnings");
    } else {
        main_pb.set_message("Bob account created successfully");
    }

    // Create Alice account
    main_pb.set_message("Creating test account: Alice...");
    let alice_success = execute_command_with_progress(
        &mpb,
        "Alice account creation",
        "npx",
        &["hyli-wallet-cli", "register", "alice", "hylisecure", "vip"]
    ).await?;

    if !alice_success {
        log_warning("Alice account creation completed with warnings");
    } else {
        main_pb.set_message("Alice account created successfully");
    }

    // Clear the main progress bar
    main_pb.finish_and_clear();

    // Clear all progress bars from the multi-progress
    mpb.clear().map_err(|e| HylixError::process(format!("Failed to clear progress bars: {}", e)))?;

    Ok(())
}

/// Send funds to test accounts
pub async fn send_funds_to_test_accounts(mpb: &indicatif::MultiProgress, _context: &DevnetContext) -> HylixResult<()> {
    let pb = mpb.add(create_progress_bar());
    pb.set_message("Sending funds to test accounts...");

    let bob_success = execute_command_with_progress(
        &mpb,
        "Bob account funding",
        "npx",
        &["hyli-wallet-cli", "transfer", "hyli", "hylisecure", "1000", "oranj", "bob@wallet"]
    ).await?;

    if !bob_success {
        log_warning("Bob account funding completed with warnings");
    } else {
        pb.set_message("Bob account funded successfully");
    }

    pb.set_message("Sending funds to Alice account...");

    let alice_success = execute_command_with_progress(
        &mpb,
        "Alice account funding",
        "npx",
        &["hyli-wallet-cli", "transfer", "hyli", "hylisecure", "1000", "oranj", "alice@wallet"]
    ).await?;

    if !alice_success {
        log_warning("Alice account funding completed with warnings");
    } else {
        pb.set_message("Alice account funded successfully");
    }

    mpb.clear().map_err(|e| HylixError::process(format!("Failed to clear progress bars: {}", e)))?;

    Ok(())
}

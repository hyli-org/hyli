use std::time::Duration;

use tracing_subscriber::{fmt, prelude::*, EnvFilter, Registry};

/// Initialize logging for the Hylix CLI
pub fn init_logging(verbose: bool, quiet: bool) -> color_eyre::Result<()> {
    let filter = if quiet {
        EnvFilter::new("error")
    } else if verbose {
        EnvFilter::new("debug")
    } else {
        EnvFilter::new("info")
    };

    let subscriber = Registry::default()
        .with(filter)
        .with(
            fmt::layer()
                .with_target(false)
                .with_thread_ids(false)
                .with_thread_names(false)
                .with_file(false)
                .with_line_number(false)
                .with_ansi(true)
                .without_time()
                .compact(),
        );

    tracing::subscriber::set_global_default(subscriber)?;

    Ok(())
}

/// Create a progress bar for long-running operations
pub fn create_progress_bar(message: &str) -> indicatif::ProgressBar {
    let pb = indicatif::ProgressBar::new_spinner();
    pb.enable_steady_tick(Duration::from_millis(100));
    pb.set_style(
        indicatif::ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
    );
    pb.set_message(message.to_string());
    pb
}

/// Log a success message with green color
pub fn log_success(message: &str) {
    tracing::info!("{} {}", console::style("✓").green(), message);
}

/// Log an error message with red color
pub fn log_error(message: &str) {
    tracing::error!("{} {}", console::style("✗").red(), message);
}

/// Log a warning message with yellow color
pub fn log_warning(message: &str) {
    tracing::warn!("{} {}", console::style("⚠").yellow(), message);
}

/// Log an info message with blue color
pub fn log_info(message: &str) {
    tracing::info!("{} {}", console::style("ℹ").blue(), message);
}

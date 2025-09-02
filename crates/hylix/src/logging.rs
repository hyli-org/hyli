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
                .with_ansi(false) // Disable ANSI in tracing-subscriber to avoid conflicts
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

/// Log a success message with green color using console directly
pub fn log_success(message: &str) {
    if console::Term::stdout().features().colors_supported() {
        println!("{} {}", console::style("✓").green(), message);
    } else {
        println!("✓ {}", message);
    }
}

/// Log an error message with red color using console directly
pub fn log_error(message: &str) {
    if console::Term::stdout().features().colors_supported() {
        eprintln!("{} {}", console::style("✗").red(), message);
    } else {
        eprintln!("✗ {}", message);
    }
}

/// Log a warning message with yellow color using console directly
pub fn log_warning(message: &str) {
    if console::Term::stdout().features().colors_supported() {
        println!("{} {}", console::style("⚠").yellow(), message);
    } else {
        println!("⚠ {}", message);
    }
}

/// Log an info message with blue color using console directly
pub fn log_info(message: &str) {
    if console::Term::stdout().features().colors_supported() {
        println!("{} {}", console::style("ℹ").blue(), message);
    } else {
        println!("ℹ {}", message);
    }
}

use std::time::Duration;

use tracing_subscriber::{fmt, prelude::*, EnvFilter, Registry};

use crate::{error::HylixError, error::HylixResult};

/// Initialize logging for the Hylix CLI
pub fn init_logging(verbose: bool, quiet: bool) -> color_eyre::Result<()> {
    let filter = if quiet {
        EnvFilter::new("error")
    } else if verbose {
        EnvFilter::new("debug")
    } else {
        EnvFilter::new("info")
    };

    let subscriber = Registry::default().with(filter).with(
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

pub fn create_progress_bar() -> indicatif::ProgressBar {
    let pb = indicatif::ProgressBar::new_spinner();
    pb.enable_steady_tick(Duration::from_millis(100));
    pb.set_style(
        indicatif::ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
    );
    pb
}

/// Create a progress bar for long-running operations
pub fn create_progress_bar_with_msg(message: &str) -> indicatif::ProgressBar {
    let pb = create_progress_bar();
    pb.set_message(message.to_string());
    pb
}

/// Helper function to execute a command with real-time progress output
pub async fn execute_command_with_progress(
    multi_progress: &indicatif::MultiProgress,
    command_name: &str,
    program: &str,
    args: &[&str],
    current_dir: Option<&str>,
) -> HylixResult<bool> {
    use std::collections::VecDeque;
    use std::io::{BufRead, BufReader};
    use std::process::Command;
    use tokio::sync::mpsc;

    let mut cmd = Command::new(program)
        .current_dir(current_dir.unwrap_or("."))
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| HylixError::process(format!("Failed to spawn {}: {}", command_name, e)))?;

    // Create progress bars dynamically as output arrives
    let mut output_bars = Vec::new();
    let max_lines = 15;

    // Create initial progress bar for the command name
    let initial_pb = multi_progress.add(indicatif::ProgressBar::new(1));
    let template = console::style("  {msg}").blue().to_string();

    initial_pb.set_style(
        indicatif::ProgressStyle::default_bar()
            .template(&template)
            .unwrap()
            .progress_chars("  "),
    );

    initial_pb.set_message(format!("{}: Starting...", command_name));
    output_bars.push(initial_pb);

    // Create channels for stdout and stderr
    let (tx, mut rx) = mpsc::channel::<(String, bool)>(100); // (line, is_stderr)
    let tx_stdout = tx.clone();
    let tx_stderr = tx;

    // Handle stdout
    if let Some(stdout) = cmd.stdout.take() {
        tokio::spawn(async move {
            let reader = BufReader::new(stdout);
            for line in reader.lines().map_while(Result::ok) {
                let _ = tx_stdout.send((line, false)).await;
            }
        });
    }

    // Handle stderr
    if let Some(stderr) = cmd.stderr.take() {
        tokio::spawn(async move {
            let reader = BufReader::new(stderr);
            for line in reader.lines().map_while(Result::ok) {
                let _ = tx_stderr.send((line, true)).await;
            }
        });
    }

    // Collect and display output in chronological order
    let mut output_buffer = VecDeque::new();
    let mut display_buffer = VecDeque::new();

    // Process output as it comes in
    while let Some((line, is_stderr)) = rx.recv().await {
        output_buffer.push_back((line.clone(), is_stderr));
        display_buffer.push_back((line.clone(), is_stderr));

        // Keep only the last 15 lines for display purposes
        if display_buffer.len() > max_lines {
            display_buffer.pop_front();
        }

        // Create new progress bar if we have more output than progress bars
        while output_bars.len() < output_buffer.len() && output_bars.len() < max_lines {
            let new_pb = multi_progress.add(indicatif::ProgressBar::new(1));
            let template = console::style("  {msg}").blue().to_string();

            new_pb.set_style(
                indicatif::ProgressStyle::default_bar()
                    .template(&template)
                    .unwrap()
                    .progress_chars("  "),
            );

            output_bars.push(new_pb);
        }

        // Update all progress bars to show the current output
        for (i, pb) in output_bars.iter().enumerate() {
            if i < display_buffer.len() {
                let (buf_line, buf_is_stderr) = &display_buffer[i];
                let message = if *buf_is_stderr {
                    format!("[stderr] {}", buf_line)
                } else {
                    buf_line.clone()
                };
                pb.set_message(message);
            } else {
                pb.set_message("");
            }
        }
    }

    // Wait for command to complete
    let status = cmd
        .wait()
        .map_err(|e| HylixError::process(format!("Failed to wait for {}: {}", command_name, e)))?;

    // Give a moment to see the final output
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Clear all output progress bars
    for pb in &output_bars {
        pb.finish_and_clear();
    }

    // If status is not success, print all logs with log_warning
    if !status.success() {
        log_warning(&format!(
            "Command '{}' failed with status: {}",
            command_name, status
        ));
        for (line, is_stderr) in &output_buffer {
            let prefix = if *is_stderr { "[stderr]" } else { "[stdout]" };
            log_warning(&format!("{} {}", prefix, line));
        }
    }

    Ok(status.success())
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

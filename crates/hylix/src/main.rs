use clap::{Parser, Subcommand};
use color_eyre::Result;
use tracing::info;

mod commands;
mod config;
mod error;
mod logging;

use config::BackendType as ConfigBackendType;

/// Build, test & deploy verifiable apps on Hyli
/// 
/// Hylix is a developer toolbox and CLI to build vApps on Hyli,
/// the new proof-powered L1 to build the next generation of apps onchain.
#[derive(Parser)]
#[command(
    name = "hy",
    version,
    about,
    long_about = None,
    arg_required_else_help = true
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Enable verbose output
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Enable quiet output (suppress all output except errors)
    #[arg(short, long, global = true)]
    quiet: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new vApp project
    New {
        /// Project name
        name: String,
        /// Choose SP1 or Risc0 for your backend
        #[arg(long, value_enum)]
        backend: Option<ConfigBackendType>,
    },
    /// Build the project
    Build {
        /// Clean build artifacts before building
        #[arg(long)]
        clean: bool,
    },
    /// Run end-to-end tests
    Test {
        /// Keep devnet alive after tests complete
        #[arg(long)]
        keep_alive: bool,
    },
    /// Start backend service
    Run {
        /// Register and interact with contracts on the public Hyli testnet
        #[arg(long)]
        testnet: bool,
        /// Automatically rebuild and re-register on file changes
        #[arg(long)]
        watch: bool,
    },
    /// Manage local devnet
    Devnet {
        #[command(subcommand)]
        action: DevnetAction,
    },
    /// Clean build artifacts
    Clean,
}



#[derive(Subcommand)]
enum DevnetAction {
    /// Start the local devnet
    Start {
        /// Reset to fresh state
        #[arg(long)]
        reset: bool,
    },
    /// Stop the local devnet
    Stop,
    /// Check the status of the local devnet
    Status,
    /// Restart the local devnet
    Restart {
        /// Reset to fresh state
        #[arg(long)]
        reset: bool,
    },
    /// Fork a running network
    Fork {
        /// Network endpoint to fork
        endpoint: String,
    },
}



#[tokio::main]
async fn main() -> Result<()> {
    // Initialize error handling
    color_eyre::install()?;

    // Parse CLI arguments
    let cli = Cli::parse();

    // Initialize logging
    logging::init_logging(cli.verbose, cli.quiet)?;

    // Execute the command
    match cli.command {
        Commands::New { name, backend } => {
            commands::new::execute(name, backend).await?;
        }
        Commands::Build { clean } => {
            commands::build::execute(clean).await?;
        }
        Commands::Test { keep_alive } => {
            commands::test::execute(keep_alive).await?;
        }
        Commands::Run { testnet, watch } => {
            commands::run::execute(testnet, watch).await?;
        }
        Commands::Devnet { action } => {
            let devnet_action = match action {
                DevnetAction::Start { reset } => commands::devnet::DevnetAction::Start { reset },
                DevnetAction::Stop => commands::devnet::DevnetAction::Stop,
                DevnetAction::Restart { reset } => commands::devnet::DevnetAction::Restart { reset },
                DevnetAction::Status => commands::devnet::DevnetAction::Status,
                DevnetAction::Fork { endpoint } => commands::devnet::DevnetAction::Fork { endpoint },
            };
            commands::devnet::execute(devnet_action).await?;
        }
        Commands::Clean => {
            commands::clean::execute().await?;
        }
    }

    info!("Command completed successfully");
    Ok(())
}

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
    #[command(alias = "n")]
    New {
        /// Project name
        name: String,
        /// Choose SP1 or Risc0 for your backend
        #[arg(long, value_enum)]
        backend: Option<ConfigBackendType>,
    },
    /// Build the project
    #[command(alias = "b")]
    Build {
        /// Clean build artifacts before building
        #[arg(long)]
        clean: bool,
    },
    /// Run end-to-end tests
    #[command(alias = "t")]
    Test {
        /// Keep devnet alive after tests complete
        #[arg(long)]
        keep_alive: bool,
    },
    /// Start backend service
    #[command(alias = "r")]
    Run {
        /// Register and interact with contracts on the public Hyli testnet
        #[arg(long)]
        testnet: bool,
        /// Automatically rebuild and re-register on file changes
        #[arg(long)]
        watch: bool,
    },
    /// Manage local devnet
    #[command(alias = "d")]
    Devnet {
        #[command(subcommand)]
        action: DevnetAction,
    },
    /// Clean build artifacts
    #[command(alias = "c")]
    Clean,
}



#[derive(Subcommand)]
enum DevnetAction {
    /// Start the local devnet
    #[command(alias = "s")]
    Start {
        /// Reset to fresh state
        #[arg(long)]
        reset: bool,
        /// Create and fund test accounts after starting devnet
        #[arg(long)]
        bake: bool,
    },
    /// Stop the local devnet
    #[command(alias = "st")]
    Stop,
    /// Check the status of the local devnet
    #[command(alias = "stat")]
    Status,
    /// Restart the local devnet
    #[command(alias = "rs")]
    Restart {
        /// Reset to fresh state
        #[arg(long)]
        reset: bool,
        /// Create and fund test accounts after restarting devnet
        #[arg(long)]
        bake: bool,
    },
    /// Create and fund test accounts
    #[command(alias = "b")]
    Bake,
    /// Fork a running network
    #[command(alias = "f")]
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
                DevnetAction::Start { reset, bake } => commands::devnet::DevnetAction::Start { reset, bake },
                DevnetAction::Stop => commands::devnet::DevnetAction::Stop,
                DevnetAction::Restart { reset, bake } => commands::devnet::DevnetAction::Restart { reset, bake },
                DevnetAction::Status => commands::devnet::DevnetAction::Status,
                DevnetAction::Fork { endpoint } => commands::devnet::DevnetAction::Fork { endpoint },
                DevnetAction::Bake => commands::devnet::DevnetAction::Bake,
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

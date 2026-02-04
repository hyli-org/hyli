use clap::{Parser, Subcommand};
use color_eyre::Result;

mod commands;
mod config;
mod constants;
mod docker;
mod env_builder;
mod error;
mod logging;
mod process;
mod validation;

use commands::{config::ConfigAction, contract::ContractAction, devnet::DevnetAction};
use config::BackendType as ConfigBackendType;
use hylix::logging::log_error;

/// Build, test & deploy verifiable apps on Hyli
///
/// Hylix is a developer toolbox and CLI to build apps on Hyli,
/// a high-performance blockchain with built-in privacy.
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
        /// Build frontend
        #[arg(long)]
        front: bool,
    },
    /// Run end-to-end tests
    #[command(alias = "t")]
    Test {
        /// Keep backend alive after tests complete.
        /// Devnet is always kept alive. You can stop it with `hy devnet down` if needed.
        #[arg(long)]
        keep_alive: bool,
        /// Run e2e tests only
        #[arg(long)]
        e2e: bool,
        /// Run unit tests only
        #[arg(long)]
        unit: bool,
        /// Extra arguments to pass to cargo test (e.g., --test-threads=1, --nocapture)
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        extra_args: Vec<String>,
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
        /// Extra arguments to pass to cargo run (e.g., --bin server)
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        extra_args: Vec<String>,
    },
    /// Manage local devnet
    #[command(alias = "d")]
    Devnet {
        #[command(subcommand)]
        action: DevnetAction,
    },
    /// Manage contracts
    #[command(alias = "c")]
    Contract {
        #[command(subcommand)]
        action: ContractAction,
    },
    Clean,
    /// Display and manage configuration
    Config {
        #[command(subcommand)]
        action: ConfigAction,
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

    // Execute the command and handle errors
    let result = match cli.command {
        Commands::New { name, backend } => commands::new::execute(name, backend).await,
        Commands::Build { clean, front } => commands::build::execute(clean, front).await,
        Commands::Test {
            keep_alive,
            e2e,
            unit,
            extra_args,
        } => commands::test::execute(keep_alive, e2e, unit, extra_args).await,
        Commands::Run {
            testnet,
            watch,
            extra_args,
        } => commands::run::execute(testnet, watch, extra_args).await,
        Commands::Devnet { action } => commands::devnet::execute(action).await,
        Commands::Clean => commands::clean::execute().await,
        Commands::Config { action } => commands::config::execute(action).await,
        Commands::Contract { action } => commands::contract::execute(action).await,
    };

    if let Err(err) = result {
        log_error(&format!("{err:#}"));
        std::process::exit(1);
    }

    Ok(())
}

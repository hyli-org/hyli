use clap::{Parser, Subcommand};
use color_eyre::Result;

mod commands;
mod config;
mod error;
mod logging;

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

#[derive(Subcommand)]
enum DevnetAction {
    /// Start the local devnet
    #[command(alias = "u")]
    Up {
        /// Reset to fresh state
        #[arg(long)]
        reset: bool,
        /// Create and fund test accounts after starting devnet
        #[arg(long)]
        bake: bool,
        /// Profile to use for baking (e.g., --profile=bobalice)
        #[arg(long, value_name = "PROFILE")]
        profile: Option<String>,
        /// No pull docker images (use existing local)
        #[arg(long)]
        no_pull: bool,
    },
    /// Stop the local devnet
    #[command(alias = "d")]
    Down,
    /// Pause the local devnet
    #[command(alias = "p")]
    Pause,
    /// Check the status of the local devnet
    #[command(alias = "ps")]
    Status,
    /// Restart the local devnet
    #[command(alias = "r")]
    Restart {
        /// Reset to fresh state
        #[arg(long)]
        reset: bool,
        /// Create and fund test accounts after restarting devnet
        #[arg(long)]
        bake: bool,
        /// Profile to use for baking (e.g., --profile=bobalice)
        #[arg(long, value_name = "PROFILE")]
        profile: Option<String>,
        /// No pull docker images (use existing local)
        #[arg(long)]
        no_pull: bool,
    },
    /// Create and fund test accounts
    #[command(alias = "b")]
    Bake {
        /// Profile to use for baking
        profile: Option<String>,
    },
    /// Fork a running network
    #[command(alias = "f")]
    Fork {
        /// Network endpoint to fork
        endpoint: String,
    },
    /// Print environment variables for sourcing in bash
    #[command(alias = "e")]
    Env,
    /// Follow logs of a devnet service
    #[command(alias = "l")]
    Logs {
        /// Service to follow logs for
        service: String,
    },
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Display current configuration
    Show,
    /// Edit configuration values
    #[command(alias = "set")]
    Edit {
        /// Configuration key to edit (e.g., "default_backend", "devnet.node_port")
        key: String,
        /// New value for the configuration key
        value: String,
    },
    /// Reset configuration to defaults
    Reset,
}

#[derive(Subcommand)]
enum ContractAction {
    /// Delete a contract
    #[command(alias = "d")]
    Delete {
        /// Contract name
        name: String,
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
        Commands::Devnet { action } => {
            let devnet_action = match action {
                DevnetAction::Up {
                    reset,
                    bake,
                    profile,
                    no_pull,
                } => commands::devnet::DevnetAction::Up {
                    reset,
                    bake,
                    profile,
                    no_pull,
                },
                DevnetAction::Down => commands::devnet::DevnetAction::Down,
                DevnetAction::Pause => commands::devnet::DevnetAction::Pause,
                DevnetAction::Restart {
                    reset,
                    bake,
                    profile,
                    no_pull,
                } => commands::devnet::DevnetAction::Restart {
                    reset,
                    bake,
                    profile,
                    no_pull,
                },
                DevnetAction::Status => commands::devnet::DevnetAction::Status,
                DevnetAction::Fork { endpoint } => {
                    commands::devnet::DevnetAction::Fork { endpoint }
                }
                DevnetAction::Bake { profile } => commands::devnet::DevnetAction::Bake { profile },
                DevnetAction::Env => commands::devnet::DevnetAction::Env,
                DevnetAction::Logs { service } => commands::devnet::DevnetAction::Logs { service },
            };
            commands::devnet::execute(devnet_action).await
        }
        Commands::Clean => commands::clean::execute().await,
        Commands::Config { action } => {
            let config_action = match action {
                ConfigAction::Show => commands::config::ConfigAction::Show,
                ConfigAction::Edit { key, value } => {
                    commands::config::ConfigAction::Edit { key, value }
                }
                ConfigAction::Reset => commands::config::ConfigAction::Reset,
            };
            commands::config::execute(config_action).await
        }
        Commands::Contract { action } => {
            let contract_action = match action {
                ContractAction::Delete { name } => {
                    commands::contract::ContractAction::Delete { name }
                }
            };
            commands::contract::execute(contract_action).await
        }
    };

    if let Err(err) = result {
        log_error(&format!("{err:#}"));
        std::process::exit(1);
    }

    Ok(())
}

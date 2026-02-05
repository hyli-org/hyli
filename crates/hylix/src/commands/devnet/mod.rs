mod config;
mod containers;
mod context;
mod logs;
mod multi_node;
mod network;
mod services;
mod single_node;
mod status;
mod utils;

pub use config::get_node_id;
pub use context::{DevnetContext, MultiNodeConfig, NodePorts};
pub use utils::is_devnet_responding;

use crate::commands::bake::bake_devnet;
use crate::config::HylixConfig;
use crate::constants;
use crate::error::{HylixError, HylixResult};
use crate::logging::{log_info, ProgressExecutor};

use containers::{get_docker_container_status, start_containers, ContainerStatus};
use logs::{logs_all_nodes, logs_devnet};
use multi_node::{join_devnet, start_multi_node_devnet};
use single_node::{fork_devnet, pause_devnet, restart_devnet, start_devnet, stop_devnet};
use status::check_devnet_status;
use utils::print_devnet_env_vars;

/// Devnet action enum
#[derive(Debug, Clone, clap::Subcommand)]
pub enum DevnetAction {
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
        /// Number of validator nodes for multi-node devnet (includes 1 local node by default)
        #[arg(long, value_name = "COUNT")]
        nodes: Option<u32>,
        /// Run all nodes in Docker (no local node for debugging)
        #[arg(long)]
        no_local: bool,
        /// Don't run extra services (indexer, registry, wallet)
        #[arg(long)]
        bare: bool,
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
        /// Don't run extra services (indexer, registry, wallet)
        #[arg(long)]
        bare: bool,
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
        /// Service to follow logs for (e.g., node, node-1, node-2, indexer)
        service: Option<String>,
        /// Follow logs of all nodes (multi-node only)
        #[arg(long)]
        all: bool,
    },
    /// Start the local node to join a multi-node devnet
    #[command(alias = "j")]
    Join {
        /// Only print the cargo command without executing
        #[arg(long)]
        dry_run: bool,
        /// Build in release mode
        #[arg(long)]
        release: bool,
        /// Resume with existing data_node_local/ instead of resetting
        #[arg(long)]
        resume: bool,
        /// Additional arguments to pass to the hyli binary
        #[arg(last = true)]
        extra_args: Vec<String>,
    },
}

/// Execute the `hy devnet` command
pub async fn execute(action: DevnetAction) -> HylixResult<()> {
    // Load configuration once
    let config = HylixConfig::load()?;
    let context = DevnetContext::new(config)?;

    match action {
        DevnetAction::Up {
            reset,
            bake,
            profile,
            no_pull,
            nodes,
            no_local,
            bare,
        } => {
            // Multi-node mode
            if let Some(node_count) = nodes {
                if node_count < 2 {
                    return Err(HylixError::devnet(
                        "Multi-node devnet requires at least 2 nodes".to_string(),
                    ));
                }
                let multi_node = MultiNodeConfig::new(node_count, no_local);
                let mut context_with_profile =
                    DevnetContext::new_with_profile(context.config, profile)?;
                if no_pull {
                    context_with_profile.without_pull();
                }
                context_with_profile.with_multi_node(multi_node);
                start_multi_node_devnet(reset, bake, &context_with_profile, bare).await?;
                return Ok(());
            }

            // Single-node mode (existing behavior)
            start_containers(&context).await?;

            if is_devnet_responding(&context).await? {
                log_info("Devnet is running");
                return Ok(());
            }
            let mut context_with_profile =
                DevnetContext::new_with_profile(context.config, profile)?;
            if no_pull {
                context_with_profile.without_pull();
            }
            start_devnet(reset, bake, &context_with_profile, bare).await?;
        }
        DevnetAction::Pause => {
            pause_devnet(&context).await?;
        }
        DevnetAction::Down => {
            stop_devnet(&context).await?;
        }
        DevnetAction::Restart {
            reset,
            bake,
            profile,
            no_pull,
            bare,
        } => {
            let mut context_with_profile =
                DevnetContext::new_with_profile(context.config, profile)?;
            if no_pull {
                context_with_profile.without_pull();
            }
            restart_devnet(reset, bake, &context_with_profile, bare).await?;
        }
        DevnetAction::Status => {
            // Multi-node Devnet
            let node_1_status =
                get_docker_container_status(&constants::containers::node_name(1)).await?;
            if node_1_status != ContainerStatus::NotExisting {
                // if node-0 exists, then we have a local node
                let no_local = get_docker_container_status(&constants::containers::node_name(0))
                    .await?
                    != ContainerStatus::NotExisting;

                // get the number of nodes by checking existing containers
                let mut node_count = 1;
                while get_docker_container_status(&constants::containers::node_name(node_count))
                    .await?
                    != ContainerStatus::NotExisting
                {
                    node_count += 1;
                }

                log_info(&format!(
                    "Detected multi-node devnet with {} nodes",
                    node_count
                ));

                // Create a context with multi-node config
                let mut multi_node_context = context;
                let multi_node = MultiNodeConfig::new(node_count, no_local);
                multi_node_context.with_multi_node(multi_node);
                check_devnet_status(&multi_node_context).await?;
                return Ok(());
            }

            check_devnet_status(&context).await?;
        }
        DevnetAction::Fork { endpoint } => {
            fork_devnet(&endpoint).await?;
        }
        DevnetAction::Bake { profile } => {
            let context_with_profile = DevnetContext::new_with_profile(context.config, profile)?;
            let executor = ProgressExecutor::new();
            bake_devnet(&executor, &context_with_profile).await?;
            executor.clear()?;
        }
        DevnetAction::Env => {
            print_devnet_env_vars(&context.config)?;
        }
        DevnetAction::Logs { service, all } => {
            if all {
                logs_all_nodes().await?;
            } else {
                let service_name = service.unwrap_or_else(|| "node".to_string());
                logs_devnet(&service_name).await?;
            }
        }
        DevnetAction::Join {
            dry_run,
            release,
            extra_args,
            resume,
        } => {
            join_devnet(dry_run, resume, release, extra_args).await?;
        }
    }

    Ok(())
}

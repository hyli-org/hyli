use crate::constants;
use crate::docker::ContainerSpec;
use crate::env_builder::EnvBuilder;
use crate::error::HylixResult;
use crate::logging::ProgressExecutor;

use crate::commands::devnet::containers::{start_managed_container, stop_and_remove_container};
use crate::commands::devnet::context::{DevnetContext, NodePorts};

/// Setup wallet app
pub async fn start_wallet_app(
    executor: &ProgressExecutor,
    context: &DevnetContext,
) -> HylixResult<()> {
    let image = &context.config.devnet.wallet_server_image;

    let (node_name, ports) = if context.multi_node.is_some() {
        (
            constants::containers::node_name(1),
            NodePorts::for_docker_node(1, &context.config.devnet),
        )
    } else {
        (
            constants::containers::NODE.to_string(),
            NodePorts::for_docker_node(0, &context.config.devnet),
        )
    };

    let env_builder = EnvBuilder::new()
        .set(
            constants::env_vars::RISC0_DEV_MODE,
            constants::env_values::RISC0_DEV_MODE_TRUE,
        )
        .set(
            constants::env_vars::HYLI_NODE_URL,
            &format!("http://{node_name}:{}", ports.rest),
        )
        .set(
            constants::env_vars::HYLI_INDEXER_URL,
            &format!("http://{}:4321", constants::containers::INDEXER),
        )
        .set(
            constants::env_vars::HYLI_DA_READ_FROM,
            &format!("{node_name}:{}", ports.da),
        )
        .set(
            constants::env_vars::HYLI_REGISTRY_URL,
            &format!(
                "http://{}:{}",
                constants::containers::REGISTRY,
                context.config.devnet.registry_server_port
            ),
        )
        .set(
            constants::env_vars::HYLI_REGISTRY_API_KEY,
            constants::env_values::REGISTRY_API_KEY_DEV,
        );

    let spec = ContainerSpec::new(constants::containers::WALLET, image)
        .port(context.config.devnet.wallet_api_port, 4000)
        .env_builder(env_builder)
        .custom_env(context.config.devnet.container_env.wallet_server.clone())
        .args(vec![
            "/app/server".to_string(),
            "-m".to_string(),
            "-w".to_string(),
            "-a".to_string(),
        ]);

    start_managed_container(executor, spec, context.pull).await?;

    start_wallet_ui(executor, context).await
}

async fn start_wallet_ui(executor: &ProgressExecutor, context: &DevnetContext) -> HylixResult<()> {
    let image = &context.config.devnet.wallet_ui_image;

    let env_builder = EnvBuilder::new()
        .set(
            "NODE_BASE_URL",
            &format!("http://localhost:{}", context.config.devnet.node_port),
        )
        .set(
            "WALLET_SERVER_BASE_URL",
            &format!("http://localhost:{}", context.config.devnet.wallet_api_port),
        )
        .set(
            "WALLET_WS_URL",
            &format!("ws://localhost:{}", context.config.devnet.wallet_ws_port),
        )
        .set(
            "INDEXER_BASE_URL",
            &format!("http://localhost:{}", context.config.devnet.indexer_port),
        )
        .set("TX_EXPLORER_URL", "https://explorer.hyli.org/");

    let spec = ContainerSpec::new(constants::containers::WALLET_UI, image)
        .port(context.config.devnet.wallet_ui_port, 80)
        .env_builder(env_builder)
        .custom_env(context.config.devnet.container_env.wallet_ui.clone());

    start_managed_container(executor, spec, context.pull).await?;

    Ok(())
}

/// Stop the wallet app
pub async fn stop_wallet_app(pb: &indicatif::ProgressBar) -> HylixResult<()> {
    // Stop and remove wallet app
    stop_and_remove_container(pb, constants::containers::WALLET, "Hyli wallet app").await?;

    // Stop and remove wallet UI
    stop_and_remove_container(pb, constants::containers::WALLET_UI, "Hyli wallet UI").await?;

    Ok(())
}

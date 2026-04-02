use crate::constants;
use crate::docker::ContainerSpec;
use crate::env_builder::EnvBuilder;
use crate::error::HylixResult;
use crate::logging::ProgressExecutor;

use crate::commands::devnet::containers::{start_managed_container, stop_and_remove_container};
use crate::commands::devnet::context::{DevnetContext, NodePorts};
use crate::commands::devnet::services::postgres::start_postgres_server;

/// Start the indexer
pub async fn start_indexer(
    executor: &ProgressExecutor,
    context: &DevnetContext,
) -> HylixResult<()> {
    let image = &context.config.devnet.node_image;

    start_postgres_server(executor, context).await?;

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
        .set(constants::env_vars::HYLI_RUN_INDEXER, "true")
        .set(
            constants::env_vars::HYLI_DATABASE_URL,
            &format!(
                "postgresql://postgres:postgres@{}:5432/hyli_indexer",
                constants::containers::POSTGRES
            ),
        )
        .set(
            constants::env_vars::HYLI_DA_READ_FROM,
            &format!("{node_name}:{}", ports.da),
        )
        .rust_log(&context.config.devnet.node_rust_log);

    let spec = ContainerSpec::new(constants::containers::INDEXER, image)
        .port(context.config.devnet.indexer_port, 4321)
        .env_builder(env_builder)
        .custom_env(context.config.devnet.container_env.indexer.clone())
        .arg("/hyli/indexer".to_string());

    start_managed_container(executor, spec, context.pull).await?;

    Ok(())
}

/// Stop the indexer
pub async fn stop_indexer(pb: &indicatif::ProgressBar) -> HylixResult<()> {
    // Stop and remove indexer
    stop_and_remove_container(pb, constants::containers::INDEXER, "Hyli indexer").await?;

    // Stop and remove postgres server
    super::postgres::stop_postgres_server(pb).await?;

    Ok(())
}

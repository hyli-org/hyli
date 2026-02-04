use crate::constants;
use crate::docker::ContainerSpec;
use crate::env_builder::EnvBuilder;
use crate::error::HylixResult;
use crate::logging::ProgressExecutor;

use crate::commands::devnet::containers::{start_managed_container, stop_and_remove_container};
use crate::commands::devnet::context::DevnetContext;

/// Start the registry app
pub async fn start_registry(
    executor: &ProgressExecutor,
    context: &DevnetContext,
) -> HylixResult<()> {
    let image = &context.config.devnet.registry_server_image;

    let env_builder = EnvBuilder::new().set(
        constants::env_vars::HYLI_REGISTRY_API_KEY,
        constants::env_values::REGISTRY_API_KEY_DEV,
    );

    let spec = ContainerSpec::new(constants::containers::REGISTRY, image)
        .port(context.config.devnet.registry_server_port, 9003)
        .env_builder(env_builder)
        .custom_env(context.config.devnet.container_env.registry_server.clone())
        .arg("/app/server".to_string());

    start_managed_container(executor, spec, context.pull).await?;

    start_registry_ui(executor, context).await
}

async fn start_registry_ui(
    executor: &ProgressExecutor,
    context: &DevnetContext,
) -> HylixResult<()> {
    let image = &context.config.devnet.registry_ui_image;

    let env_builder = EnvBuilder::new().set(
        "REGISTRY_SERVER_BASE_URL",
        &format!(
            "http://localhost:{}",
            context.config.devnet.registry_server_port
        ),
    );

    let spec = ContainerSpec::new(constants::containers::REGISTRY_UI, image)
        .port(context.config.devnet.registry_ui_port, 80)
        .env_builder(env_builder);

    start_managed_container(executor, spec, context.pull).await?;

    Ok(())
}

/// Stop the registry
pub async fn stop_registry(pb: &indicatif::ProgressBar) -> HylixResult<()> {
    // Stop and remove registry server
    stop_and_remove_container(pb, constants::containers::REGISTRY, "Hyli registry server").await?;

    // Stop and remove registry UI
    stop_and_remove_container(pb, constants::containers::REGISTRY_UI, "Hyli registry UI").await?;

    Ok(())
}

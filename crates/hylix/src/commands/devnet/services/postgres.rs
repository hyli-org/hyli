use crate::constants;
use crate::docker::ContainerSpec;
use crate::env_builder::EnvBuilder;
use crate::error::HylixResult;
use crate::logging::ProgressExecutor;

use crate::commands::devnet::containers::{start_managed_container, stop_and_remove_container};
use crate::commands::devnet::context::DevnetContext;
use crate::commands::devnet::utils::wait_for_postgres_server;

/// Start the postgres server
pub async fn start_postgres_server(
    executor: &ProgressExecutor,
    context: &DevnetContext,
) -> HylixResult<()> {
    let env_builder = EnvBuilder::new()
        .set("POSTGRES_USER", "postgres")
        .set("POSTGRES_PASSWORD", "postgres")
        .set("POSTGRES_DB", "hyli_indexer");

    let spec = ContainerSpec::new(constants::containers::POSTGRES, constants::images::POSTGRES)
        .port(context.config.devnet.postgres_port, 5432)
        .env_builder(env_builder)
        .custom_env(context.config.devnet.container_env.postgres.clone());

    start_managed_container(executor, spec, context.pull).await?;

    let pb = executor.add_task("Waiting for postgres server to be ready...");
    wait_for_postgres_server(&pb).await?;
    pb.finish_and_clear();

    Ok(())
}

/// Stop the postgres server
pub async fn stop_postgres_server(pb: &indicatif::ProgressBar) -> HylixResult<()> {
    stop_and_remove_container(pb, constants::containers::POSTGRES, "Hyli postgres server").await
}

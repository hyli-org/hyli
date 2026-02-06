use crate::constants;
use crate::docker::ContainerSpec;
use crate::env_builder::EnvBuilder;
use crate::error::HylixResult;
use crate::logging::ProgressExecutor;

use crate::commands::devnet::containers::start_managed_container;
use crate::commands::devnet::context::DevnetContext;
use crate::commands::devnet::utils::wait_for_block_height;

/// Start the local node
pub async fn start_local_node(
    executor: &ProgressExecutor,
    context: &DevnetContext,
) -> HylixResult<()> {
    let image = &context.config.devnet.node_image;

    let env_builder = EnvBuilder::new()
        .risc0_dev_mode()
        .sp1_prover_mock()
        .set(constants::env_vars::HYLI_RUN_INDEXER, "false")
        .set(constants::env_vars::HYLI_RUN_EXPLORER, "false")
        .rust_log(&context.config.devnet.node_rust_log);

    let spec = ContainerSpec::new(constants::containers::NODE, image)
        .port(context.config.devnet.node_port, 4321)
        .port(context.config.devnet.da_port, 4141)
        .env_builder(env_builder)
        .custom_env(context.config.devnet.container_env.node.clone());

    start_managed_container(executor, spec, context.pull).await?;

    // Wait for the node to be ready by checking block height
    let pb = executor.add_task("Waiting for node to be ready...");
    wait_for_block_height(&pb, context, 2).await?;
    pb.finish_and_clear();

    Ok(())
}

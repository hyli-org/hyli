use crate::constants;
use crate::error::HylixResult;
use crate::logging::{log_info, log_success, log_warning};
use client_sdk::rest_client::NodeApiClient;

use super::containers::{check_docker_container, ContainerStatus};
use super::context::{DevnetContext, NodePorts};
use super::utils::is_devnet_responding;

/// Check the status of the local devnet
pub async fn check_devnet_status(context: &DevnetContext) -> HylixResult<()> {
    let multi_node = context.multi_node.is_some();

    if multi_node {
        log_info("Checking status of multi-node devnet...");
    } else {
        log_info("Checking status of single-node devnet...");
    }

    let containers = if multi_node {
        let mut all_containers = Vec::new();
        let multi_node_config = context.multi_node.as_ref().unwrap();
        let start_index = if multi_node_config.has_local_node {
            1
        } else {
            0
        };
        for i in start_index..multi_node_config.total_nodes {
            all_containers.push(constants::containers::node_name(i));
        }
        all_containers.extend_from_slice(&[
            constants::containers::POSTGRES.to_string(),
            constants::containers::INDEXER.to_string(),
            constants::containers::REGISTRY.to_string(),
            constants::containers::REGISTRY_UI.to_string(),
            constants::containers::WALLET.to_string(),
            constants::containers::WALLET_UI.to_string(),
        ]);
        all_containers
    } else {
        constants::containers::ALL
            .to_vec()
            .iter()
            .map(|s| s.to_string())
            .collect()
    };

    for container in &containers {
        check_docker_container(context, container).await?;
    }

    let is_running = check_docker_container(context, &containers[0]).await?
        == ContainerStatus::Running
        && is_devnet_responding(context).await?;

    if is_running {
        log_success("Devnet is running");

        let block_height = context.client.get_block_height().await?;
        log_info(&format!("Block height: {}", block_height.0));

        log_info("Services available at:");
        log_info(&format!(
            "  Node: http://localhost:{}/swagger-ui",
            context.config.devnet.node_port
        ));
        log_info(&format!(
            "  Explorer: https://explorer.hyli.org/?network=localhost&indexer={}&node={}&wallet={}",
            context.config.devnet.indexer_port,
            context.config.devnet.node_port,
            context.config.devnet.wallet_api_port
        ));
        log_info(&format!(
            "  Indexer: http://localhost:{}/swagger-ui",
            context.config.devnet.indexer_port
        ));
        log_info(&format!(
            "  Registry API: http://localhost:{}/swagger-ui",
            context.config.devnet.registry_server_port
        ));
        log_info(&format!(
            "  Registry UI: http://localhost:{}",
            context.config.devnet.registry_ui_port
        ));
        log_info(&format!(
            "  Wallet API: http://localhost:{}/swagger-ui",
            context.config.devnet.wallet_api_port
        ));
        log_info(&format!(
            "  Wallet UI: http://localhost:{}",
            context.config.devnet.wallet_ui_port
        ));
    } else {
        log_warning("Devnet is not running");
    }

    if multi_node {
        print_multi_node_status(context)?;
    }
    Ok(())
}

/// Print multi-node devnet status
pub fn print_multi_node_status(context: &DevnetContext) -> HylixResult<()> {
    let multi_node = context.multi_node.as_ref().unwrap();

    log_info("Nodes:");
    for i in 0..multi_node.docker_nodes {
        let node_index = if multi_node.has_local_node { i + 1 } else { i };
        let ports = NodePorts::for_docker_node(node_index, &context.config.devnet);
        println!(
            "  ● node-{}       docker   REST: {}   P2P: {}",
            node_index, ports.rest, ports.p2p
        );
    }

    if multi_node.has_local_node {
        let ports = NodePorts::for_local_node(&context.config.devnet);
        println!(
            "  ○ node-local   local    REST: {}   P2P: {}",
            ports.rest, ports.p2p
        );
    }

    Ok(())
}

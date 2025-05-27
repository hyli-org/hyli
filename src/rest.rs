pub use hyle_modules::modules::rest::*;

#[cfg(test)]
mod tests {
    use crate::{
        bus::bus_client, data_availability::DataAvailability, genesis::Genesis,
        mempool::api::RestApiMessage, model::SharedRunContext, node_state::module::NodeStateModule,
        p2p::P2P, single_node_consensus::SingleNodeConsensus, tcp_server::TcpServer,
        utils::integration_test::NodeIntegrationCtxBuilder,
    };
    use anyhow::Result;
    use client_sdk::rest_client::{NodeApiClient, NodeApiHttpClient};
    use hyle_model::*;
    use hyle_modules::{
        bus::SharedMessageBus,
        module_handle_messages,
        modules::{signal::ShutdownModule, Module},
    };
    use std::time::Duration;
    use tracing::info;

    bus_client! {
        struct MockBusClient {
            receiver(RestApiMessage),
            receiver(ShutdownModule),
        }
    }

    // Mock module that listens to REST API
    struct RestApiListener {
        bus: MockBusClient,
    }

    impl Module for RestApiListener {
        type Context = SharedRunContext;

        async fn build(bus: SharedMessageBus, _ctx: Self::Context) -> Result<Self> {
            Ok(RestApiListener {
                bus: MockBusClient::new_from_bus(bus.new_handle()).await,
            })
        }

        async fn run(&mut self) -> Result<()> {
            module_handle_messages! {
                on_bus self.bus,
                listen<RestApiMessage> msg => {
                    info!("Received REST API message: {:?}", msg);
                    // Just listen to messages to skip the shutdown timer.
                }
            };

            Ok(())
        }
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_rest_api_shutdown_with_mocked_modules() {
        // Create a new integration test context with all modules mocked except REST API
        let builder = NodeIntegrationCtxBuilder::new().await;
        let rest_client = builder.conf.rest_server_port;

        // Mock Genesis with our RestApiListener, and skip other modules except mempool (for its API)
        let builder = builder
            .with_mock::<Genesis, RestApiListener>()
            .skip::<SingleNodeConsensus>()
            .skip::<DataAvailability>()
            .skip::<NodeStateModule>()
            .skip::<P2P>()
            .skip::<TcpServer>();

        let node = builder.build().await.expect("Failed to build node");

        let client = NodeApiHttpClient::new(format!("http://localhost:{}", rest_client))
            .expect("Failed to create client");

        node.wait_for_rest_api(&client).await.unwrap();

        // Spawn a task to send requests
        let request_handle = tokio::spawn({
            let client = NodeApiHttpClient::new(format!("http://localhost:{}", rest_client))
                .expect("Failed to create client");
            let dummy_tx = BlobTransaction::new(
                "test@identity",
                vec![Blob {
                    contract_name: "identity".into(),
                    data: BlobData(vec![0, 1, 2, 3]),
                }],
            );
            async move {
                let mut request_count = 0;
                while client.send_tx_blob(dummy_tx.clone()).await.is_ok() {
                    request_count += 1;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                request_count
            }
        });

        // Let it send a few requests.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Drop the node context, which will trigger graceful shutdown
        drop(node);

        // Wait for the request task to complete and get the request count
        let request_count = request_handle.await.expect("Request task failed");

        // Ensure we made at least a few successful requests
        assert!(request_count > 0, "No successful requests were made");

        // Verify server has stopped by attempting a request
        // Create a client using NodeApiHttpClient
        let err = client
            .get_node_info()
            .await
            .expect_err("Expected request to fail after shutdown");
        let err = format!("{:#}", err);
        assert!(
            err.to_string().contains("getting node info"),
            "Expected connection error, got: {}",
            err
        );
        assert!(
            err.to_string().contains("Connection refused"),
            "Expected connection error, got: {}",
            err
        );
    }
}
